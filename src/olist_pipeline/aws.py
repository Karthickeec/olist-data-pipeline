"""AWS target: bucket naming, Secrets Manager references and the S3A settings for Spark.

Credentials always come from the default chain (the local AWS profile, or a role on AWS);
nothing here reads, prints or stores keys. Secret values are wrapped in `Secret`, whose repr
is masked, so a config dump or traceback never shows them.
"""
import json
from functools import lru_cache

SECRET_SCHEME = "secret://"
# Short region codes for bucket names (bucket names are global, so they carry account and region).
REGION_CODES = {"ap-southeast-2": "apse2", "ap-south-1": "aps1", "us-east-1": "use1"}
HADOOP_AWS_PACKAGES = ("org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262")


class Secret(str):
    """A str whose repr is masked: usable as a password, never shown in config dumps."""

    def __repr__(self) -> str:
        return "'***'"


def session(region: str):
    import boto3  # imported lazily: local runs and tests don't need AWS
    return boto3.session.Session(region_name=region)


@lru_cache(maxsize=None)
def account_id(region: str) -> str:
    return session(region).client("sts").get_caller_identity()["Account"]


def bucket_name(aws: dict, account: str | None = None) -> str:
    """<prefix>-<account id>-<region code>; the account id is looked up, never committed."""
    if aws.get("bucket"):
        return aws["bucket"]
    region = aws["region"]
    return f"{aws['bucket_prefix']}-{account or account_id(region)}-{REGION_CODES.get(region, region)}"


def parse_secret_ref(ref: str) -> tuple[str, str]:
    """secret://<secret id>#<json field> -> (secret id, field)."""
    if not ref.startswith(SECRET_SCHEME) or "#" not in ref:
        raise ValueError(f"expected secret://<id>#<field>, got {ref.split('#')[0]!r}")
    secret_id, _, field = ref[len(SECRET_SCHEME):].partition("#")
    if not secret_id or not field:
        raise ValueError("expected secret://<id>#<field>")
    return secret_id, field


@lru_cache(maxsize=None)
def _secret_json(secret_id: str, region: str) -> dict:
    """One GetSecretValue call per secret per process."""
    value = session(region).client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"]
    return json.loads(value)


def resolve_secret(ref: str, region: str, fetch=None) -> Secret:
    secret_id, field = parse_secret_ref(ref)
    data = (fetch or _secret_json)(secret_id, region)
    if field not in data:
        raise KeyError(f"field {field!r} not found in secret {secret_id!r}")
    return Secret(data[field])


def resolve_secrets(node: dict, region: str, fetch=None) -> None:
    """Replace every secret://… string leaf of a config dict, in place."""
    for key, value in node.items():
        if isinstance(value, dict):
            resolve_secrets(value, region, fetch)
        elif isinstance(value, str) and value.startswith(SECRET_SCHEME):
            node[key] = resolve_secret(value, region, fetch)


def apply_aws_target(cfg: dict, bucket: str | None = None) -> None:
    """OLIST_TARGET=aws: the lake moves to S3 and the credentials come from Secrets Manager."""
    aws = cfg["aws"]
    cfg["lake"]["root"] = f"s3a://{bucket or bucket_name(aws)}/lake"
    ref = f"{SECRET_SCHEME}{aws['secret_id']}"
    cfg["pg"]["password"] = f"{ref}#pg_password"
    cfg["api"]["key"] = f"{ref}#api_key"


def s3a_conf(region: str) -> dict:
    """Spark settings for s3a:// paths. No keys: the SDK's default chain finds the profile or role."""
    return {
        "spark.hadoop.fs.s3a.endpoint": f"s3.{region}.amazonaws.com",
        "spark.hadoop.fs.s3a.endpoint.region": region,
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "spark.hadoop.fs.s3a.connection.maximum": "64",
        # The S3A magic/staging committers don't support Spark's dynamic partition overwrite, so writes
        # keep the classic FileOutputCommitter (rename = copy on S3; fine at this size).
        "spark.hadoop.fs.s3a.committer.name": "file",
    }


def to_s3_uri(uri: str) -> str:
    """s3a://… (Hadoop) -> s3://… (CLI, Athena)."""
    return "s3://" + uri.removeprefix("s3a://") if uri.startswith("s3a://") else uri
