use std::{borrow::Cow, ops::Deref, sync::Arc};

use headers::{ContentLength, ContentType, HeaderMapExt};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use hyper::{
    client::{Client, HttpConnector},
    Body, Request, StatusCode,
};
use hyper_tls::HttpsConnector;

type HttpsClient = Client<HttpsConnector<HttpConnector>>;

#[derive(Debug)]
pub struct B2ClientSession {
    pub config: B2ClientConfig,
    pub session: B2Session,
}

impl Deref for B2ClientSession {
    type Target = B2Session;

    fn deref(&self) -> &B2Session {
        &self.session
    }
}

#[derive(Clone)]
pub struct B2Client {
    inner: HttpsClient,
    pub session: Arc<B2ClientSession>,
}

#[derive(Debug)]
pub struct B2ClientConfig {
    pub app_id: String,
    pub app_key: String,
    pub bucket: String,
}

impl B2ClientConfig {
    fn encode(&self) -> String {
        base64::encode(format!("{}:{}", self.app_id, self.app_key))
    }
}

#[derive(Debug, Deserialize)]
pub struct B2Session {
    #[serde(rename = "accountId")]
    pub account_id: String,

    #[serde(rename = "authorizationToken")]
    pub auth_token: String,

    #[serde(rename = "apiUrl")]
    pub api_url: String,

    #[serde(rename = "downloadUrl")]
    pub download_url: String,

    #[serde(rename = "recommendedPartSize")]
    pub recommended_part_size: usize,

    #[serde(rename = "absoluteMinimumPartSize")]
    pub absolute_min_part_size: usize,

    #[serde(rename = "s3ApiUrl")]
    pub s3_api_url: String,

    pub allowed: Allowed,
}

#[derive(Debug, Deserialize)]
pub struct Allowed {
    #[serde(default, rename = "bucketId")]
    pub bucket_id: Option<String>,

    #[serde(default, rename = "bucketName")]
    pub bucket_name: Option<String>,

    pub capabilities: Vec<Capabilities>,

    #[serde(default, rename = "namePrefix")]
    pub name_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
pub enum Capabilities {
    #[serde(rename = "writeBucketEncryption")]
    WriteBucketEncryption,
    #[serde(rename = "readBucketEncryption")]
    ReadBucketEncryption,
    #[serde(rename = "readBuckets")]
    ReadBuckets,
    #[serde(rename = "listKeys")]
    ListKeys,
    #[serde(rename = "writeKeys")]
    WriteKeys,
    #[serde(rename = "deleteKeys")]
    DeleteKeys,
    #[serde(rename = "listBuckets")]
    ListBuckets,
    #[serde(rename = "writeBuckets")]
    WriteBuckets,
    #[serde(rename = "deleteBuckets")]
    DeleteBuckets,
    #[serde(rename = "listFiles")]
    ListFiles,
    #[serde(rename = "readFiles")]
    ReadFiles,
    #[serde(rename = "shareFiles")]
    ShareFiles,
    #[serde(rename = "writeFiles")]
    WriteFiles,
    #[serde(rename = "deleteFiles")]
    DeleteFiles,
}

#[derive(Debug, thiserror::Error)]
pub enum B2Error {
    #[error("HTTP Error: {0}")]
    Http(#[from] hyper::http::Error),

    #[error("Client Error: {0}")]
    Client(#[from] hyper::Error),

    #[error("Json Error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Url-encoding Error: {0}")]
    UrlEncoded(#[from] serde_urlencoded::ser::Error),

    #[error("B2 Failure: {0:?}")]
    Failure(B2ErrorMessage),

    #[error("Server Error: {0}")]
    ServerError(StatusCode),

    #[error("Specified bucket is not allowed")]
    BucketNotAllowed,
}

#[derive(Debug, Deserialize)]
pub struct B2ErrorMessage {
    pub status: u16,
    pub code: String,
    pub message: String,
}

use bytes::{Buf, Bytes};
use futures::Stream;

async fn aggregate_body(body: &mut Body) -> Result<impl Buf, hyper::Error> {
    hyper::body::aggregate(std::mem::replace(body, Body::empty())).await
}

async fn parse_body<T: DeserializeOwned>(status: StatusCode, body: &mut Body) -> Result<T, B2Error> {
    if status.is_server_error() {
        return Err(B2Error::ServerError(status));
    }

    let bytes = aggregate_body(body).await?;

    if status.is_client_error() {
        Err(B2Error::Failure(serde_json::from_reader(bytes.reader())?))
    } else {
        Ok(serde_json::from_reader(bytes.reader())?)
    }
}

impl B2Client {
    pub async fn connect(config: B2ClientConfig) -> Result<B2Client, B2Error> {
        let client = Client::builder().build(HttpsConnector::new());

        let mut auth_res = client
            .request(
                Request::get("https://api.backblazeb2.com/b2api/v2/b2_authorize_account")
                    .header("Authorization", format!("Basic {}", config.encode()))
                    .body(Body::empty())?,
            )
            .await?;

        let session: B2Session = parse_body(auth_res.status(), auth_res.body_mut()).await?;

        if session.allowed.bucket_name.is_none()
            || session.allowed.bucket_name.as_ref() != Some(&config.bucket)
        {
            return Err(B2Error::BucketNotAllowed);
        }

        Ok(B2Client {
            inner: client,
            session: Arc::new(B2ClientSession { config, session }),
        })
    }

    pub async fn get_upload(&self) -> Result<B2Upload, B2Error> {
        #[derive(Serialize)]
        struct GetUploadUrlParams<'a> {
            #[serde(rename = "bucketId")]
            bucket_id: &'a str,
        }

        let body = Body::from(serde_urlencoded::to_string(&GetUploadUrlParams {
            bucket_id: &self.session.allowed.bucket_id.as_ref().unwrap(),
        })?);

        let mut get_upload_url_request =
            Request::post(format!("{}/b2api/v2/b2_get_upload_url", self.session.api_url))
                .header("Authorization", &self.session.auth_token);

        let headers = get_upload_url_request.headers_mut().unwrap();
        headers.typed_insert(ContentType::form_url_encoded());

        let mut result = self.inner.request(get_upload_url_request.body(body)?).await?;

        let upload_url = parse_body(result.status(), result.body_mut()).await?;

        Ok(B2Upload {
            client: self.clone(),
            upload_url,
        })
    }
}

#[derive(Deserialize)]
struct GetUploadUrlResult {
    #[serde(rename = "bucketId")]
    bucket_id: String,
    #[serde(rename = "uploadUrl")]
    upload_url: String,
    #[serde(rename = "authorizationToken")]
    authorization_token: String,
}

pub struct B2Upload {
    client: B2Client,
    upload_url: GetUploadUrlResult,
}

impl B2Upload {
    pub async fn upload(
        self,
        path: &str,
        sha1: &[u8; 20],
        length: u64,
        stream: impl Stream<Item = Bytes>,
    ) -> Result<(), B2Error> {
        let encoded_name = urlencoding::encode(&path);
        let encoded_sha1 = base64::encode(sha1);

        //headers.typed_insert(ContentLength(length));

        Ok(())
    }
}
