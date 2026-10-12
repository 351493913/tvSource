# -*- coding: utf-8 -*-
"""一键更新直播源并上传到 GitHub。

用法：
    python update_and_upload.py

步骤：
  1. 运行 fetch_tv_m3u8.py 采集全国电视台直播源（自动检测可用性、剔除失效链接）
  2. 把生成的 tv_channels.txt 上传覆盖到 GitHub 仓库 351493913/tvSource

GitHub 令牌的两种配置方式（任选其一）：
  A. 系统环境变量 GITHUB_TOKEN（推荐，较安全）
  B. 本脚本同目录下创建 upload_token.txt，把令牌粘进去（注意别把这个文件传到网上）
"""

import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO = "351493913/tvSource"
FILE_NAME = "tv_channels.txt"
FILE_PATH = os.path.join(SCRIPT_DIR, FILE_NAME)
API_URL = "https://api.github.com/repos/%s/contents/%s" % (REPO, FILE_NAME)


def get_token():
    token = (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    if token:
        return token
    cfg = os.path.join(SCRIPT_DIR, "upload_token.txt")
    if os.path.exists(cfg):
        with open(cfg, encoding="utf-8") as f:
            token = f.read().strip()
        if token:
            return token
    return None


def api_call(url, method="GET", payload=None, token=None):
    headers = {
        "Authorization": "Bearer %s" % token,
        "User-Agent": "trae-agent",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, None


def main():
    token = get_token()
    if not token:
        print("没有找到 GitHub 令牌，请任选一种方式配置后重试：")
        print("  1. 设置系统环境变量 GITHUB_TOKEN（推荐）")
        print("  2. 在本脚本同目录创建 upload_token.txt 并粘贴令牌")
        sys.exit(1)

    print("[1/2] 开始采集直播源并检测可用性...")
    cmd = [
        sys.executable, os.path.join(SCRIPT_DIR, "fetch_tv_m3u8.py"),
        "--workers", "128", "--timeout", "5", "--max-per-channel", "8",
    ]
    result = subprocess.run(cmd, cwd=SCRIPT_DIR)
    if result.returncode != 0:
        print("采集失败，终止上传。")
        sys.exit(1)

    if not os.path.exists(FILE_PATH):
        print("未找到 %s，采集可能没有产出文件。" % FILE_NAME)
        sys.exit(1)
    size = os.path.getsize(FILE_PATH)
    print("[2/2] 上传 %s（%d 字节）到 GitHub..." % (FILE_NAME, size))

    # 先查仓库里旧文件的 sha（更新文件时 GitHub 要求提供）
    code, info = api_call(API_URL, "GET", token=token)
    sha = info.get("sha") if info else None

    with open(FILE_PATH, "rb") as f:
        content = base64.b64encode(f.read()).decode("ascii")
    payload = {"message": "update %s" % FILE_NAME, "content": content}
    if sha:
        payload["sha"] = sha

    code, info = None, None
    for attempt in range(1, 4):  # 上传失败自动重试，最多 3 次
        code, info = api_call(API_URL, "PUT", payload, token)
        if code in (200, 201) and info:
            break
        print("  上传失败（HTTP %s），%d 秒后第 %d 次重试..." % (code, 10, attempt))
        time.sleep(10)
    if code in (200, 201) and info:
        sha = (info.get("content") or {}).get("sha") or info.get("sha")
        print("上传成功！")
        print("  仓库: https://github.com/%s" % REPO)
        print("  文件: %s" % FILE_NAME)
        print("  新版本 sha: %s" % sha)
    else:
        print("上传失败：HTTP %s。请检查令牌是否还有效、权限是否为 Contents 读写。" % code)
        sys.exit(1)


if __name__ == "__main__":
    main()
