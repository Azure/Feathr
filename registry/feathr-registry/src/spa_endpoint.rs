use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

use poem::{
    error::StaticFileError, http::Method, web::StaticFileRequest, Endpoint, FromRequest,
    IntoResponse, Request, Response, Result,
};

pub struct SpaEndpoint {
    path: PathBuf,
    index_path: PathBuf,
    prefer_utf8: bool,
}

impl SpaEndpoint {
    pub fn new(path: impl Into<PathBuf>, index_file: impl ToString) -> Self {
        let path: PathBuf = path.into();
        let index_path = path.join(index_file.to_string());
        Self {
            path,
            index_path,
            prefer_utf8: true,
        }
    }
}

#[async_trait::async_trait]
impl Endpoint for SpaEndpoint {
    type Output = Response;

    async fn call(&self, req: Request) -> Result<Self::Output> {
        if req.method() != Method::GET {
            return Err(StaticFileError::MethodNotAllowed(req.method().clone()).into());
        }

        let path = req
            .uri()
            .path()
            .trim_start_matches('/')
            .trim_end_matches('/');

        let path = percent_encoding::percent_decode_str(path)
            .decode_utf8()
            .map_err(|_| StaticFileError::InvalidPath)?;

        let mut file_path = self.path.clone();
        for p in Path::new(&*path) {
            if p == OsStr::new(".") {
                continue;
            } else if p == OsStr::new("..") {
                file_path.pop();
            } else {
                file_path.push(&p);
            }
        }

        if !file_path.starts_with(&self.path) {
            return Err(StaticFileError::Forbidden(file_path.display().to_string()).into());
        }

        if file_path.exists() && file_path.is_file() {
            return Ok(StaticFileRequest::from_request_without_body(&req)
                .await?
                .create_response(&file_path, self.prefer_utf8)?
                .into_response());
        } else {
            return Ok(StaticFileRequest::from_request_without_body(&req)
                .await?
                .create_response(&self.index_path, self.prefer_utf8)?
                .into_response());
        }
    }
}
