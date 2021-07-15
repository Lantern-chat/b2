use std::sync::Arc;

use serde::Deserialize;

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

#[derive(Clone)]
pub struct B2Client {
    pub client: HttpsClient,
    pub session: Arc<B2ClientSession>,
}

#[derive(Debug)]
pub struct B2ClientConfig {
    pub app_id: String,
    pub app_key: String,
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

    #[error("B2 Failure: {0:?}")]
    Failure(B2ErrorMessage),

    #[error("Server Error: {0}")]
    ServerError(StatusCode),
}

#[derive(Debug, Deserialize)]
pub struct B2ErrorMessage {
    pub status: u16,
    pub code: String,
    pub message: String,
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

        let status = auth_res.status();

        if status.is_server_error() {
            return Err(B2Error::ServerError(status));
        }

        let body = aggregate_body(auth_res.body_mut()).await?.reader();

        if status.is_client_error() {
            return Err(B2Error::Failure(serde_json::from_reader(body)?));
        }

        let session = serde_json::from_reader(body)?;

        Ok(B2Client {
            client,
            session: Arc::new(B2ClientSession { config, session }),
        })
    }
}

use bytes::Buf;

async fn aggregate_body(body: &mut Body) -> Result<impl Buf, hyper::Error> {
    hyper::body::aggregate(std::mem::replace(body, Body::empty())).await
}
