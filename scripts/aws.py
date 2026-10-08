"""AWS resources for the project (ap-southeast-2; everything tagged project=olist-pipeline).

    aws.py check                 read-only probes of the services the project uses
    aws.py up                    deploy infra/step8.yaml (bucket, Athena workgroup, Glue database) + the secret
    aws.py sync                  upload the local lake and landing files to S3
    aws.py publish               incremental sync after a local run: changed files up, replaced files deleted
    aws.py status                stack, bucket size and every resource tagged project=olist-pipeline
    aws.py glue-test             smallest possible Glue Spark job (Flex, 2 x G.1X, 5-min timeout)
    aws.py glue-deploy           build the wheel, upload it with the entry script and DQ suites, create job olist-spark
    aws.py down [--dry-run]      teardown: pull the lake back, delete Glue test, secret, bucket contents, stack

Credentials come from the default chain (AWS profile); secret values are never printed.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from botocore.exceptions import ClientError

from olist_pipeline.aws import account_id, bucket_name, session
from olist_pipeline.config import PROJECT_ROOT, load_config

STACK = "olist-pipeline-step8"
TEMPLATE = PROJECT_ROOT / "infra" / "step8.yaml"
GLUE_JOB = "olist-glue-test"
GLUE_ROLE = "olist-glue-test"
GLUE_SCRIPT = PROJECT_ROOT / "infra" / "glue_test_job.py"
PIPELINE_JOB = "olist-spark"
PIPELINE_ROLE = "olist-glue"
PIPELINE_ENTRY = PROJECT_ROOT / "infra" / "glue_job.py"
DQ_SUITES = PROJECT_ROOT / "config" / "dq"
# Installed on Glue next to the wheel (Glue 5.0 = Python 3.11); the versions installed locally.
GLUE_PYPI_MODULES = ("psycopg[binary]==3.3.6", "PyYAML==6.0.3")
FLEX_DPU_HOUR = 0.29  # ap-southeast-2, Glue 5.0 Flex (AWS Pricing API, 2026-10-09)
SYNC_EXCLUDES = ("*.crc", "*/_staging/*", "*.DS_Store")


class Ctx:
    def __init__(self):
        self.cfg = load_config()  # local target: the source of the values that go into the secret
        self.aws = self.cfg["aws"]
        self.region = self.aws["region"]
        self.session = session(self.region)
        self.account = account_id(self.region)
        self.bucket = bucket_name(self.aws, self.account)
        self.tags = self.aws["tags"]

    def client(self, name: str):
        return self.session.client(name)

    def tag_list(self, key="Key", value="Value") -> list[dict]:
        return [{key: k, value: v} for k, v in self.tags.items()]


def masked(text: str, ctx: "Ctx") -> str:
    """Never show the account id (it is part of the bucket name)."""
    return text.replace(ctx.account, "<account>")


def sh(ctx: "Ctx", *args: str, check: bool = True) -> int:
    print("$ " + masked(" ".join(args), ctx))
    out = subprocess.run(args, check=check, capture_output=True, text=True)
    for line in (out.stdout + out.stderr).splitlines():
        if line.strip():
            print(masked(line, ctx))
    return out.returncode


def cmd_check(ctx: Ctx, _args) -> None:
    probes = {
        "s3 list": lambda: ctx.client("s3").list_buckets(),
        "athena": lambda: ctx.client("athena").list_work_groups(),
        "glue catalog": lambda: ctx.client("glue").get_databases(),
        "glue jobs": lambda: ctx.client("glue").get_jobs(),
        "secrets manager": lambda: ctx.client("secretsmanager").list_secrets(),
        "cloudformation": lambda: ctx.client("cloudformation").list_stacks(),
        "tagging api": lambda: ctx.client("resourcegroupstaggingapi").get_resources(ResourcesPerPage=1),
        "emr serverless": lambda: ctx.client("emr-serverless").list_applications(),
    }
    print(f"region {ctx.region}, caller {masked(ctx.client('sts').get_caller_identity()['Arn'], ctx)}")
    for name, call in probes.items():
        try:
            call()
            print(f"  OK        {name}")
        except ClientError as e:
            msg = str(e)
            print(f"  {'SCP deny' if 'service control policy' in msg else e.response['Error']['Code']:9s} {name}")


def ensure_secret(ctx: Ctx) -> None:
    sm = ctx.client("secretsmanager")
    key_env = ctx.cfg["api"]["key_env"]
    api_key = os.environ.get(key_env)
    if not api_key:
        sys.exit(f"{key_env} is not set (the Makefile sets the local default)")
    value = json.dumps({"pg_password": ctx.cfg["pg"]["password"], "api_key": api_key})
    secret_id = ctx.aws["secret_id"]
    try:
        current = sm.get_secret_value(SecretId=secret_id)["SecretString"]
        if current == value:
            print(f"secret {secret_id}: up to date")
        else:
            sm.put_secret_value(SecretId=secret_id, SecretString=value)
            print(f"secret {secret_id}: new version stored")
    except sm.exceptions.ResourceNotFoundException:
        sm.create_secret(
            Name=secret_id,
            SecretString=value,
            Tags=ctx.tag_list(),
            Description="olist-pipeline: Postgres password and mock-API key",
        )
        print(f"secret {secret_id}: created")


def cmd_up(ctx: Ctx, _args) -> None:
    sh(
        ctx,
        "aws",
        "cloudformation",
        "deploy",
        "--region",
        ctx.region,
        "--stack-name",
        STACK,
        "--template-file",
        str(TEMPLATE),
        "--no-fail-on-empty-changeset",
        "--parameter-overrides",
        f"BucketName={ctx.bucket}",
        f"GlueDatabaseName={ctx.aws['glue_database']}",
        f"WorkGroupName={ctx.aws['athena_workgroup']}",
        "--tags",
        *(f"{k}={v}" for k, v in ctx.tags.items()),
    )
    ensure_secret(ctx)
    cmd_status(ctx, None)


def _excludes() -> list[str]:
    return [a for e in SYNC_EXCLUDES for a in ("--exclude", e)]


def cmd_sync(ctx: Ctx, _args) -> None:
    for local, prefix in ((ctx.cfg["lake"]["root"], "lake"), (ctx.cfg["paths"]["landing_dir"], "landing")):
        sh(
            ctx,
            "aws",
            "s3",
            "sync",
            f"{local}/",
            f"s3://{ctx.bucket}/{prefix}/",
            "--region",
            ctx.region,
            "--only-show-errors",
            *_excludes(),
        )
    cmd_status(ctx, None)


def cmd_publish(ctx: Ctx, _args) -> None:
    """Compute stays local (see README: ~0.4 s per S3 round trip from here); S3 gets the result.

    --delete removes objects whose local file was replaced (a rewritten partition gets new part-file
    names), so S3 keeps exactly the local lake. Versioning keeps the old objects for 7 days.
    """
    t0 = time.time()
    for local, prefix, delete in (
        (ctx.cfg["lake"]["root"], "lake", True),
        (ctx.cfg["paths"]["landing_dir"], "landing", False),
    ):
        out = subprocess.run(
            [
                "aws",
                "s3",
                "sync",
                f"{local}/",
                f"s3://{ctx.bucket}/{prefix}/",
                "--region",
                ctx.region,
                "--no-progress",
                *(["--delete"] if delete else []),
                *_excludes(),
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        up = sum(1 for line in out if line.startswith("upload:"))
        rm = sum(1 for line in out if line.startswith("delete:"))
        print(f"{prefix}: {up} uploaded, {rm} deleted")
    print(f"published in {time.time() - t0:.1f}s")


def cmd_pull(ctx: Ctx, _args) -> None:
    """After Glue jobs wrote Silver/Gold on S3: make the local lake equal to S3 again."""
    t0 = time.time()
    out = subprocess.run(
        [
            "aws",
            "s3",
            "sync",
            f"s3://{ctx.bucket}/lake/",
            f"{ctx.cfg['lake']['root']}/",
            "--region",
            ctx.region,
            "--no-progress",
            "--delete",
            *_excludes(),
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    down = sum(1 for line in out if line.startswith("download:"))
    rm = sum(1 for line in out if line.startswith("delete:"))
    print(f"lake: {down} downloaded, {rm} deleted locally in {time.time() - t0:.1f}s")


def bucket_usage(ctx: Ctx, prefix: str = "") -> tuple[int, int]:
    s3, n, size = ctx.client("s3"), 0, 0
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=ctx.bucket, Prefix=prefix):
            for o in page.get("Contents", []):
                n, size = n + 1, size + o["Size"]
    except s3.exceptions.NoSuchBucket:
        pass
    return n, size


def tagged_resources(ctx: Ctx) -> list[str]:
    pages = (
        ctx.client("resourcegroupstaggingapi")
        .get_paginator("get_resources")
        .paginate(TagFilters=[{"Key": k, "Values": [v]} for k, v in ctx.tags.items()])
    )
    return [r["ResourceARN"] for p in pages for r in p["ResourceTagMappingList"]]


def cmd_status(ctx: Ctx, _args) -> None:
    try:
        st = ctx.client("cloudformation").describe_stacks(StackName=STACK)["Stacks"][0]["StackStatus"]
    except ClientError:
        st = "absent"
    print(f"stack {STACK}: {st}")
    for prefix in ("lake/", "landing/", "athena-results/", "glue-test/"):
        n, size = bucket_usage(ctx, prefix)
        print(f"  s3://<bucket>/{prefix:16s} {n:6d} objects  {size / 1e6:8.1f} MB")
    arns = tagged_resources(ctx)
    print(f"resources tagged {ctx.tags}: {len(arns)}")
    for arn in arns:
        print("  " + masked(arn, ctx))


# --- Glue test job ----------------------------------------------------------------------------


def ensure_glue_role(ctx: Ctx, name: str = GLUE_ROLE, writable: tuple[str, ...] = ("glue-test",)) -> str:
    """Glue service role with S3 access to this bucket only: read lake/glue/control, write `writable` prefixes."""
    iam = ctx.client("iam")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole", "Principal": {"Service": "glue.amazonaws.com"}}],
    }
    try:
        arn = iam.get_role(RoleName=name)["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        arn = iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description=f"olist-pipeline Glue role ({name})",
            Tags=ctx.tag_list(),
        )["Role"]["Arn"]
        time.sleep(10)  # IAM is eventually consistent; Glue can't assume a role in its first seconds
    iam.attach_role_policy(RoleName=name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
    b = f"arn:aws:s3:::{ctx.bucket}"
    readable = sorted({"lake", "glue", "control", "glue-test", *writable})
    iam.put_role_policy(
        RoleName=name,
        PolicyName="olist-bucket",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": b},
                    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [f"{b}/{p}/*" for p in readable]},
                    {
                        "Effect": "Allow",
                        "Action": ["s3:PutObject", "s3:DeleteObject"],
                        "Resource": [f"{b}/{p}/*" for p in writable],
                    },
                ],
            }
        ),
    )
    return arn


def cmd_glue_deploy(ctx: Ctx, _args) -> None:
    """Package the project for Glue and create/update the generic pipeline job."""
    s3, glue = ctx.client("s3"), ctx.client("glue")
    dist = PROJECT_ROOT / "dist"
    for old in dist.glob("*.whl"):
        old.unlink()
    subprocess.run(
        [
            os.environ.get("UV", str(Path.home() / ".local/bin/uv")),
            "build",
            "--wheel",
            "--out-dir",
            str(dist),
            str(PROJECT_ROOT),
        ],
        check=True,
        capture_output=True,
    )
    wheel = next(dist.glob("olist_pipeline-*.whl"))
    s3.upload_file(str(wheel), ctx.bucket, f"glue/{wheel.name}")
    s3.upload_file(str(PIPELINE_ENTRY), ctx.bucket, "glue/glue_job.py")
    for suite in sorted(DQ_SUITES.glob("*.yaml")):
        s3.upload_file(str(suite), ctx.bucket, f"glue/dq/{suite.name}")
    role = ensure_glue_role(ctx, PIPELINE_ROLE, writable=("lake", "control"))
    job = dict(
        Role=role,
        Command={"Name": "glueetl", "ScriptLocation": f"s3://{ctx.bucket}/glue/glue_job.py", "PythonVersion": "3"},
        GlueVersion="5.0",
        WorkerType="G.1X",
        NumberOfWorkers=2,
        ExecutionClass="FLEX",
        Timeout=30,
        MaxRetries=0,
        ExecutionProperty={"MaxConcurrentRuns": 1},
        DefaultArguments={
            "--job-language": "python",
            "--additional-python-modules": ",".join([f"s3://{ctx.bucket}/glue/{wheel.name}", *GLUE_PYPI_MODULES]),
            "--LAKE": f"s3://{ctx.bucket}/lake",
            "--SUITES": f"s3://{ctx.bucket}/glue/dq/",
            "--enable-metrics": "false",
        },
    )
    try:
        glue.get_job(JobName=PIPELINE_JOB)
        glue.update_job(JobName=PIPELINE_JOB, JobUpdate=job)
        print(f"job {PIPELINE_JOB}: updated")
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(
            Name=PIPELINE_JOB,
            Tags=ctx.tags,
            Description="olist-pipeline: Silver, Gold and DQ for one batch date (--TASK, --DATE)",
            **job,
        )
        print(f"job {PIPELINE_JOB}: created")
    print(
        f"  {wheel.name}, entry script, {len(list(DQ_SUITES.glob('*.yaml')))} DQ suites uploaded to s3://<bucket>/glue/;"
        f" Glue 5.0 Flex, 2 x G.1X, timeout 30 min, 0 retries; role {PIPELINE_ROLE}"
    )


def cmd_glue_test(ctx: Ctx, args) -> None:
    glue, s3 = ctx.client("glue"), ctx.client("s3")
    role = ensure_glue_role(ctx)
    script_key = "glue-test/scripts/glue_test_job.py"
    s3.upload_file(str(GLUE_SCRIPT), ctx.bucket, script_key)
    job = dict(
        Role=role,
        Command={"Name": "glueetl", "ScriptLocation": f"s3://{ctx.bucket}/{script_key}", "PythonVersion": "3"},
        GlueVersion="5.0",
        WorkerType="G.1X",
        NumberOfWorkers=2,
        ExecutionClass="FLEX",
        Timeout=5,
        MaxRetries=0,
        ExecutionProperty={"MaxConcurrentRuns": 1},
        DefaultArguments={
            "--job-language": "python",
            "--INPUT": f"s3://{ctx.bucket}/lake/gold/dim_date/",
            "--OUTPUT": f"s3://{ctx.bucket}/glue-test/output/",
        },
    )
    try:
        glue.get_job(JobName=GLUE_JOB)
        glue.update_job(JobName=GLUE_JOB, JobUpdate=job)
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(Name=GLUE_JOB, Tags=ctx.tags, Description="olist-pipeline: smallest Spark test", **job)
    print(f"job {GLUE_JOB}: Glue 5.0, FLEX, 2 x G.1X (2 DPU), timeout 5 min, 0 retries")
    for attempt in range(6):  # a just-created role can take a few more seconds to become assumable
        try:
            run_id = glue.start_job_run(JobName=GLUE_JOB)["JobRunId"]
            break
        except ClientError as e:
            if "assume" not in str(e).lower() or attempt == 5:
                raise
            time.sleep(10)
    t0 = time.time()
    while True:
        run = glue.get_job_run(JobName=GLUE_JOB, RunId=run_id)["JobRun"]
        if run["JobRunState"] in ("SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR"):
            break
        print(f"  {time.time() - t0:5.0f}s  {run['JobRunState']}")
        time.sleep(15)
    secs = run.get("ExecutionTime", 0)
    dpu_secs = run.get("DPUSeconds") or max(secs, 60) * 2
    print(
        f"run {run_id}: {run['JobRunState']} after {time.time() - t0:.0f}s wall clock; "
        f"billed execution {secs}s, {dpu_secs:.0f} DPU-seconds -> ${dpu_secs / 3600 * FLEX_DPU_HOUR:.4f}"
    )
    if run.get("ErrorMessage"):
        print("error: " + masked(run["ErrorMessage"][:500], ctx))
    if run["JobRunState"] == "SUCCEEDED":
        for o in s3.list_objects_v2(Bucket=ctx.bucket, Prefix="glue-test/output/").get("Contents", []):
            if o["Key"].endswith(".json"):
                print("output: " + s3.get_object(Bucket=ctx.bucket, Key=o["Key"])["Body"].read().decode().strip())
    sys.exit(0 if run["JobRunState"] == "SUCCEEDED" else 1)


# --- teardown ---------------------------------------------------------------------------------


def delete_glue(ctx: Ctx, dry: bool) -> None:
    """Glue jobs and their IAM roles (IAM roles don't show up in the tagging API, so they're named here)."""
    glue, iam = ctx.client("glue"), ctx.client("iam")
    for job in (GLUE_JOB, PIPELINE_JOB):
        try:
            glue.get_job(JobName=job)
            print(f"{'would delete' if dry else 'deleting'} Glue job {job}")
            if not dry:
                glue.delete_job(JobName=job)
        except glue.exceptions.EntityNotFoundException:
            pass
    for role in (GLUE_ROLE, PIPELINE_ROLE):
        try:
            iam.get_role(RoleName=role)
            print(f"{'would delete' if dry else 'deleting'} IAM role {role}")
            if not dry:
                for p in iam.list_attached_role_policies(RoleName=role)["AttachedPolicies"]:
                    iam.detach_role_policy(RoleName=role, PolicyArn=p["PolicyArn"])
                for name in iam.list_role_policies(RoleName=role)["PolicyNames"]:
                    iam.delete_role_policy(RoleName=role, PolicyName=name)
                iam.delete_role(RoleName=role)
        except iam.exceptions.NoSuchEntityException:
            pass


def empty_bucket(ctx: Ctx, dry: bool) -> None:
    s3 = ctx.client("s3")
    try:
        pages = list(s3.get_paginator("list_object_versions").paginate(Bucket=ctx.bucket))
    except s3.exceptions.NoSuchBucket:
        return
    keys = [
        {"Key": v["Key"], "VersionId": v["VersionId"]}
        for p in pages
        for v in p.get("Versions", []) + p.get("DeleteMarkers", [])
    ]
    print(f"{'would delete' if dry else 'deleting'} {len(keys)} object versions and delete markers")
    if not dry:
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=ctx.bucket, Delete={"Objects": keys[i : i + 1000], "Quiet": True})


def cmd_down(ctx: Ctx, args) -> None:
    dry = args.dry_run
    n, _ = bucket_usage(ctx, "lake/")
    if n and not args.no_pull:
        print(f"pulling the S3 lake ({n} objects) back to {ctx.cfg['lake']['root']}")
        if not dry:
            sh(
                ctx,
                "aws",
                "s3",
                "sync",
                f"s3://{ctx.bucket}/lake/",
                f"{ctx.cfg['lake']['root']}/",
                "--delete",
                "--region",
                ctx.region,
                "--only-show-errors",
                *_excludes(),
            )
    delete_glue(ctx, dry)
    sm = ctx.client("secretsmanager")
    try:
        sm.describe_secret(SecretId=ctx.aws["secret_id"])
        print(f"{'would delete' if dry else 'deleting'} secret {ctx.aws['secret_id']} (no recovery window)")
        if not dry:
            sm.delete_secret(SecretId=ctx.aws["secret_id"], ForceDeleteWithoutRecovery=True)
    except sm.exceptions.ResourceNotFoundException:
        pass
    empty_bucket(ctx, dry)
    print(f"{'would delete' if dry else 'deleting'} stack {STACK} (bucket, workgroup, Glue database + tables)")
    if not dry:
        cf = ctx.client("cloudformation")
        cf.delete_stack(StackName=STACK)
        cf.get_waiter("stack_delete_complete").wait(StackName=STACK)
        time.sleep(5)
        left = tagged_resources(ctx)
        print(f"teardown done; resources still tagged {ctx.tags}: {len(left)}")
        for arn in left:
            print("  " + masked(arn, ctx))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("check", "up", "sync", "publish", "pull", "status", "glue-test", "glue-deploy"):
        sub.add_parser(name)
    d = sub.add_parser("down")
    d.add_argument("--dry-run", action="store_true")
    d.add_argument("--no-pull", action="store_true", help="don't copy the S3 lake back to the local lake first")
    args = p.parse_args()
    ctx = Ctx()
    {
        "check": cmd_check,
        "up": cmd_up,
        "sync": cmd_sync,
        "publish": cmd_publish,
        "status": cmd_status,
        "glue-test": cmd_glue_test,
        "glue-deploy": cmd_glue_deploy,
        "pull": cmd_pull,
        "down": cmd_down,
    }[args.cmd](ctx, args)


if __name__ == "__main__":
    main()
