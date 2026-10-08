"""After `aws.py sync`: S3 must hold exactly the local lake and landing files, byte for byte.

Compares listings instead of reading data through Spark (which costs several S3 round trips per
file): every local file needs an S3 object with the same key, size and ETag. For a single-part
upload with SSE-S3 the ETag is the file's MD5; for a multipart upload it is the MD5 of the part
MD5s plus "-<parts>", computed here with the AWS CLI's default 8 MiB part size.
"""
import hashlib
import sys
import time
from pathlib import Path

from olist_pipeline.aws import bucket_name, session
from olist_pipeline.config import load_config

PART_SIZE = 8 * 1024 * 1024   # AWS CLI default multipart_chunksize (and multipart_threshold)
EXCLUDED_SUFFIXES = (".crc", ".DS_Store")


def excluded(rel: str) -> bool:
    return rel.endswith(EXCLUDED_SUFFIXES) or "/_staging/" in f"/{rel}"


def local_etag(path: Path) -> str:
    size = path.stat().st_size
    with open(path, "rb") as f:
        if size < PART_SIZE:
            return hashlib.md5(f.read()).hexdigest()
        parts = [hashlib.md5(chunk).digest() for chunk in iter(lambda: f.read(PART_SIZE), b"")]
    return f"{hashlib.md5(b''.join(parts)).hexdigest()}-{len(parts)}"


def local_files(root: Path) -> dict[str, tuple[int, Path]]:
    return {rel: (p.stat().st_size, p) for p in root.rglob("*")
            if p.is_file() and not excluded(rel := p.relative_to(root).as_posix())}


def s3_objects(s3, bucket: str, prefix: str) -> dict[str, tuple[int, str]]:
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for o in page.get("Contents", []):
            out[o["Key"][len(prefix) + 1:]] = (o["Size"], o["ETag"].strip('"'))
    return out


def compare(name: str, local: dict, remote: dict) -> int:
    missing = sorted(local.keys() - remote.keys())
    extra = sorted(remote.keys() - local.keys())
    size_diff = sorted(k for k in local.keys() & remote.keys() if local[k][0] != remote[k][0])
    etag_diff = sorted(k for k in local.keys() & remote.keys()
                       if k not in size_diff and local_etag(local[k][1]) != remote[k][1])
    size = sum(s for s, _ in local.values())
    bad = len(missing) + len(extra) + len(size_diff) + len(etag_diff)
    print(f"{'PASS' if not bad else 'FAIL'}  {name:8s} local {len(local):5d} files {size / 1e6:7.1f} MB | "
          f"s3 {len(remote):5d} objects | missing {len(missing)}, extra {len(extra)}, "
          f"size differs {len(size_diff)}, etag differs {len(etag_diff)}")
    for label, keys in (("missing", missing), ("extra", extra), ("size", size_diff), ("etag", etag_diff)):
        for k in keys[:5]:
            print(f"      {label}: {k}")
    return bad


def main() -> None:
    cfg = load_config()
    region = cfg["aws"]["region"]
    s3, bucket = session(region).client("s3"), bucket_name(cfg["aws"])
    t0, bad = time.time(), 0
    for prefix, root in (("lake", Path(cfg["lake"]["root"])), ("landing", cfg["paths"]["landing_dir"])):
        bad += compare(prefix, local_files(root), s3_objects(s3, bucket, prefix))
    print(f"\n{'identical' if not bad else f'{bad} differences'}: keys, sizes and checksums compared "
          f"in {time.time() - t0:.1f}s")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
