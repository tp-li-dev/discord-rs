use src::event_seq::*;

fn range(first: u64, last: u64) -> SequenceRange {
    SequenceRange { first, last }
}
fn put(sender: &mut Sender, value: u8) -> u64 {
    sender
        .enqueue(vec![value], Priority::Required)
        .ok()
        .unwrap()
}

#[test]
fn ordered_admission_gap_duplicate_and_queue_backpressure() {
    let mut receiver = Receiver::new(0);
    assert_eq!(
        receiver.accept(2, true),
        Err(ReceiveError::GapDetected(range(0, 1)))
    );
    assert_eq!(receiver.accept(0, false), Err(ReceiveError::QueueFull));
    assert_eq!(receiver.accept(0, true), Ok(EventAccepted(range(0, 0))));
    assert_eq!(receiver.accept(0, true), Err(ReceiveError::Duplicate));
    assert_eq!(
        receiver.accept(2, true),
        Err(ReceiveError::GapDetected(range(1, 1)))
    );
    assert_eq!(receiver.accept(1, true), Ok(EventAccepted(range(1, 1))));
    assert_eq!(receiver.accept(2, true), Ok(EventAccepted(range(2, 2))));
}

#[test]
fn ack_ranges_and_switch_replay_are_bounded_and_ordered() {
    let mut sender = Sender::new(4, 0, 4, 10).unwrap();
    for i in 0..4 {
        assert_eq!(put(&mut sender, i), 10 + u64::from(i));
    }
    sender.ack(EventAccepted(range(11, 12))).unwrap();
    assert_eq!(sender.unacked_ranges(), vec![range(10, 10), range(13, 13)]);
    assert_eq!(sender.retained_len(), 4); // ACK metadata cannot grow past capacity.
    assert_eq!(
        sender
            .enqueue(vec![5], Priority::Required)
            .err()
            .unwrap()
            .reason,
        Rejection::Backpressure
    );
    assert_eq!(
        sender
            .replay()
            .map(|e| e.local_sequence)
            .collect::<Vec<_>>(),
        vec![10, 13]
    );
    sender.ack(EventAccepted(range(10, 10))).unwrap();
    assert_eq!(sender.accepted_prefix(), Some(12));
    assert_eq!(sender.retained_bytes(), 1);
    let mut new_core = Receiver::new(13);
    let event = sender.replay().next().unwrap();
    assert_eq!(event.payload, vec![3]);
    sender
        .ack(new_core.accept(event.local_sequence, true).unwrap())
        .unwrap();
    assert_eq!(sender.retained_len(), 0);
    assert_eq!(sender.retained_bytes(), 0);
    sender.ack(EventAccepted(range(10, 13))).unwrap();
}

#[test]
fn invalid_ack_is_atomic_and_cannot_roll_back() {
    let mut sender = Sender::new(3, 0, 3, 5).unwrap();
    put(&mut sender, 1);
    put(&mut sender, 2);
    for invalid in [range(4, 5), range(6, 5), range(5, 7), range(7, u64::MAX)] {
        assert_eq!(sender.ack(EventAccepted(invalid)), Err(Error::InvalidAck));
        assert_eq!(sender.unacked_ranges(), vec![range(5, 6)]);
    }
    sender.ack(EventAccepted(range(5, 6))).unwrap();
    assert_eq!(put(&mut sender, 3), 7);
    sender.ack(EventAccepted(range(5, 5))).unwrap();
    assert_eq!(sender.unacked_ranges(), vec![range(7, 7)]);
}

#[test]
fn reserved_capacity_full_and_loss_are_explicit() {
    let mut sender = Sender::new(2, 1, 2, 0).unwrap();
    assert_eq!(put(&mut sender, 1), 0);
    let rejected = sender.enqueue(vec![2], Priority::BestEffort).err().unwrap();
    assert_eq!(rejected.reason, Rejection::GapDetected);
    assert_eq!(rejected.payload, vec![2]);
    assert_eq!(sender.enqueue(vec![3], Priority::Interaction).ok(), Some(1));
    for priority in [Priority::Required, Priority::Interaction] {
        assert_eq!(
            sender.enqueue(vec![4], priority).err().unwrap().reason,
            Rejection::Backpressure
        );
    }
    assert_eq!(sender.retained_len(), 2);
    assert_eq!(sender.retained_bytes(), 2);
    sender.ack(EventAccepted(range(0, 1))).unwrap();
    assert_eq!(put(&mut sender, 5), 2); // Rejection did not assign a sequence.
}

#[test]
fn byte_limit_oversize_and_empty_payloads() {
    let mut sender = Sender::new(3, 0, 2, 1).unwrap();
    assert_eq!(
        sender
            .enqueue(vec![0; 3], Priority::Required)
            .err()
            .unwrap()
            .reason,
        Rejection::TooLarge
    );
    assert_eq!(sender.enqueue(vec![0; 2], Priority::Required).ok(), Some(1));
    assert_eq!(
        sender
            .enqueue(vec![0], Priority::BestEffort)
            .err()
            .unwrap()
            .reason,
        Rejection::GapDetected
    );
    assert_eq!(sender.enqueue(vec![], Priority::Required).ok(), Some(2));
    assert_eq!(sender.enqueue(vec![], Priority::Required).ok(), Some(3));
    assert_eq!(
        sender
            .enqueue(vec![], Priority::Required)
            .err()
            .unwrap()
            .reason,
        Rejection::Backpressure
    );
    assert_eq!(sender.retained_bytes(), 2);
}

#[test]
fn maximum_sequence_never_wraps() {
    let mut sender = Sender::new(2, 0, 2, u64::MAX).unwrap();
    assert_eq!(put(&mut sender, 0), u64::MAX);
    assert_eq!(
        sender
            .enqueue(vec![1], Priority::Required)
            .err()
            .unwrap()
            .reason,
        Rejection::SequenceExhausted
    );
    let mut receiver = Receiver::new(u64::MAX);
    sender
        .ack(receiver.accept(u64::MAX, true).unwrap())
        .unwrap();
    assert_eq!(
        receiver.accept(0, true),
        Err(ReceiveError::SequenceExhausted)
    );
    assert_eq!(sender.accepted_prefix(), Some(u64::MAX));
    assert_eq!(sender.retained_len(), 0);
}

#[test]
fn invalid_configuration() {
    for (capacity, reserved, bytes) in [(0, 0, 1), (1, 2, 1), (1, 0, 0)] {
        assert!(matches!(
            Sender::new(capacity, reserved, bytes, 0),
            Err(Error::InvalidConfig)
        ));
    }
}

#[test]
fn unacked_replay_after_lost_ack_rejects_duplicate_at_core() {
    let mut sender = Sender::new(2, 0, 2, 0).unwrap();
    put(&mut sender, 1);
    put(&mut sender, 2);
    let mut receiver = Receiver::new(0);
    let ack = receiver.accept(0, true).unwrap(); // ACK lost on transport.
    let mut switched = receiver.clone();
    assert_eq!(
        switched.accept(sender.replay().next().unwrap().local_sequence, true),
        Err(ReceiveError::Duplicate)
    );
    sender.ack(ack).unwrap();
    let event = sender.replay().next().unwrap();
    sender
        .ack(switched.accept(event.local_sequence, true).unwrap())
        .unwrap();
    assert!(sender.unacked_ranges().is_empty());
}
