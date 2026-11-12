//! Pure generation-switch state machine. The caller must serialize admission,
//! routing and state transitions under the same lock; this module performs no I/O.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase {
    Active,
    Prepared { candidate: u64 },
    Draining { previous: u64 },
    RollingBack { abandoned: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SwitchError {
    InvalidPhase,
    DuplicateGeneration,
    Unsettled,
}

/// Tracks routing, not readiness, IPC, persistence, or Discord REST itself.
/// Generation identifiers must not be reused within a process lifetime.
#[derive(Debug)]
pub struct Switch {
    active: u64,
    phase: Phase,
    last_generation: u64,
}

impl Switch {
    pub fn new(initial: u64) -> Self {
        Self {
            active: initial,
            phase: Phase::Active,
            last_generation: initial,
        }
    }
    pub fn active(&self) -> u64 {
        self.active
    }
    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// Invoke only after the candidate has passed readiness checks.
    pub fn prepare(&mut self, candidate: u64) -> Result<(), SwitchError> {
        if self.phase != Phase::Active {
            return Err(SwitchError::InvalidPhase);
        }
        if candidate <= self.last_generation {
            return Err(SwitchError::DuplicateGeneration);
        }
        self.last_generation = candidate;
        self.phase = Phase::Prepared { candidate };
        Ok(())
    }

    /// Commit routing atomically with closing admission to the previous generation.
    pub fn commit(&mut self) -> Result<(), SwitchError> {
        let Phase::Prepared { candidate } = self.phase else {
            return Err(SwitchError::InvalidPhase);
        };
        let previous = self.active;
        self.active = candidate;
        self.phase = Phase::Draining { previous };
        Ok(())
    }

    /// Abort a prepared switch before routing changes; old admission remains open.
    pub fn abort_prepare(&mut self) -> Result<(), SwitchError> {
        if !matches!(self.phase, Phase::Prepared { .. }) {
            return Err(SwitchError::InvalidPhase);
        }
        self.phase = Phase::Active;
        Ok(())
    }

    /// Switch back while the previous generation still exists. The caller must atomically
    /// close abandoned-generation admission and reopen the previous generation.
    pub fn rollback(&mut self) -> Result<(), SwitchError> {
        let Phase::Draining { previous } = self.phase else {
            return Err(SwitchError::InvalidPhase);
        };
        let abandoned = self.active;
        self.active = previous;
        self.phase = Phase::RollingBack { abandoned };
        Ok(())
    }

    /// Retire the draining generation only after task, REST, persistence and IPC checks.
    /// `settled` must be computed by the caller under its routing lock; unknown outcomes
    /// and timeouts are not settled.
    pub fn finish(&mut self, settled: bool) -> Result<u64, SwitchError> {
        let generation = match self.phase {
            Phase::Draining { previous } => previous,
            Phase::RollingBack { abandoned } => abandoned,
            _ => return Err(SwitchError::InvalidPhase),
        };
        if !settled {
            return Err(SwitchError::Unsettled);
        }
        self.phase = Phase::Active;
        Ok(generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn commit_and_retire_only_after_settlement() {
        let mut switch = Switch::new(1);
        switch.prepare(2).unwrap();
        assert_eq!(switch.active(), 1);
        switch.commit().unwrap();
        assert_eq!(switch.active(), 2);
        assert_eq!(switch.finish(false), Err(SwitchError::Unsettled));
        assert_eq!(switch.phase(), Phase::Draining { previous: 1 });
        assert_eq!(switch.finish(true), Ok(1));
        assert_eq!(switch.phase(), Phase::Active);
    }
    #[test]
    fn failure_before_and_after_commit_reopens_previous() {
        let mut switch = Switch::new(1);
        switch.prepare(2).unwrap();
        switch.abort_prepare().unwrap();
        assert_eq!(switch.active(), 1);
        assert_eq!(switch.prepare(2), Err(SwitchError::DuplicateGeneration));
        switch.prepare(3).unwrap();
        switch.commit().unwrap();
        switch.rollback().unwrap();
        assert_eq!(switch.active(), 1);
        assert_eq!(switch.finish(false), Err(SwitchError::Unsettled));
        assert_eq!(switch.finish(true), Ok(3));
        assert_eq!(switch.prepare(4), Ok(()));
    }
    #[test]
    fn invalid_transitions_are_rejected() {
        let mut switch = Switch::new(0);
        assert_eq!(switch.commit(), Err(SwitchError::InvalidPhase));
        assert_eq!(switch.rollback(), Err(SwitchError::InvalidPhase));
        switch.prepare(1).unwrap();
        assert_eq!(switch.prepare(2), Err(SwitchError::InvalidPhase));
        assert_eq!(switch.finish(true), Err(SwitchError::InvalidPhase));
    }
}
