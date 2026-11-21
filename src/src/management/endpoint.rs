use std::io;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
};

pub trait ControlStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> ControlStream for T {}
pub type Stream = Box<dyn ControlStream>;

pub enum Listener {
    Tcp(TcpListener),
    #[cfg(target_os = "linux")]
    Unix(linux::LocalSocket),
}

impl Listener {
    pub async fn bind_tcp(port: u16) -> io::Result<Self> {
        TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
            .await
            .map(Self::Tcp)
    }

    pub fn port(&self) -> io::Result<Option<u16>> {
        match self {
            Self::Tcp(listener) => listener.local_addr().map(|addr| Some(addr.port())),
            #[cfg(target_os = "linux")]
            Self::Unix(_) => Ok(None),
        }
    }

    pub async fn accept(&self) -> io::Result<Option<Stream>> {
        match self {
            Self::Tcp(listener) => {
                let (stream, peer) = listener.accept().await?;
                Ok(peer.ip().is_loopback().then(|| Box::new(stream) as Stream))
            }
            #[cfg(target_os = "linux")]
            Self::Unix(socket) => socket.accept().await,
        }
    }

    #[cfg(target_os = "linux")]
    pub fn bind_unix(path: &std::path::Path) -> io::Result<Self> {
        linux::LocalSocket::bind(path).map(Self::Unix)
    }
}

pub async fn connect_tcp(port: u16) -> io::Result<Stream> {
    let stream = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    Ok(Box::new(stream))
}

#[cfg(target_os = "linux")]
pub use linux::connect_unix;

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use crate::secret_file::effective_uid;
    use std::{
        fs::{self, Metadata, Permissions},
        os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
        path::{Component, Path, PathBuf},
    };
    use tokio::net::{UnixListener, UnixStream};

    fn unsafe_endpoint() -> io::Error {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unsafe local control socket path or permissions",
        )
    }

    fn validate_directories(path: &Path, uid: u32) -> io::Result<()> {
        if !path.is_absolute()
            || path.file_name().is_none()
            || path.components().any(|part| part == Component::ParentDir)
        {
            return Err(unsafe_endpoint());
        }
        let parents: Vec<_> = path.ancestors().skip(1).collect();
        for (index, directory) in parents.into_iter().enumerate().rev() {
            let meta = fs::symlink_metadata(directory).map_err(|_| unsafe_endpoint())?;
            let mode = meta.mode();
            let trusted_owner = meta.uid() == uid || meta.uid() == 0;
            let root_sticky = meta.uid() == 0 && mode & 0o1000 != 0;
            if !meta.is_dir()
                || !trusted_owner
                || (mode & 0o022 != 0 && !root_sticky)
                || (index == 0 && (meta.uid() != uid || mode & 0o7777 != 0o700))
            {
                return Err(unsafe_endpoint());
            }
        }
        Ok(())
    }

    fn socket_metadata(path: &Path, uid: u32) -> io::Result<Metadata> {
        let meta = fs::symlink_metadata(path)?;
        if !meta.file_type().is_socket() || meta.uid() != uid || meta.nlink() != 1 {
            return Err(unsafe_endpoint());
        }
        Ok(meta)
    }

    fn peer_matches(stream: &UnixStream, uid: u32) -> bool {
        stream.peer_cred().is_ok_and(|cred| cred.uid() == uid)
    }

    pub struct LocalSocket {
        listener: UnixListener,
        path: PathBuf,
        inode: (u64, u64),
        uid: u32,
    }

    impl LocalSocket {
        pub(super) fn bind(path: &Path) -> io::Result<Self> {
            let uid = effective_uid();
            validate_directories(path, uid)?;
            // bind fails if anything already exists there, including a symlink.
            let listener = UnixListener::bind(path)?;
            let meta = socket_metadata(path, uid)?;
            let socket = Self {
                listener,
                path: path.to_owned(),
                inode: (meta.dev(), meta.ino()),
                uid,
            };
            fs::set_permissions(path, Permissions::from_mode(0o600))?;
            Ok(socket)
        }

        pub(super) async fn accept(&self) -> io::Result<Option<Stream>> {
            let (stream, _) = self.listener.accept().await?;
            Ok(peer_matches(&stream, self.uid).then(|| Box::new(stream) as Stream))
        }
    }

    impl Drop for LocalSocket {
        fn drop(&mut self) {
            if validate_directories(&self.path, self.uid).is_ok()
                && socket_metadata(&self.path, self.uid)
                    .is_ok_and(|meta| (meta.dev(), meta.ino()) == self.inode)
            {
                let _ = fs::remove_file(&self.path);
            }
        }
    }

    pub async fn connect_unix(path: &Path) -> io::Result<Stream> {
        let uid = effective_uid();
        validate_directories(path, uid)?;
        let meta = socket_metadata(path, uid)?;
        if meta.mode() & 0o7777 != 0o600 {
            return Err(unsafe_endpoint());
        }
        let stream = UnixStream::connect(path).await?;
        if !peer_matches(&stream, uid) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local control socket peer UID mismatch",
            ));
        }
        Ok(Box::new(stream))
    }
}
