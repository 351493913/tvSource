#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全国电视台直播源采集脚本（仅用 Python 标准库，无需安装任何第三方包）

功能：
  1. 从多个公开直播源聚合列表抓取全国电视台直播地址（m3u8 为主）
  2. 频道名自动归一化，同一频道的多个地址合并在一起
  3. 默认逐条检测并分三级（--no-check 可关闭）：
       确认可播（列表正常且分片可取）> 降级备用（列表正常但当下未取到分片，
       这类源的分片时有时无）> 直接删除（错误码 / 网页错误页 / 无信号占位流 /
       空播放列表）。同一频道内按"确认可播优先、再按实测速度从高到低"排列
  4. 保存为电视软件通用解析格式：
       tv_channels.m3u   M3U 标准格式（VLC / PotPlayer / TVBox 直接导入）
       tv_channels.txt   DIYP / TVBox 通用格式（频道名,链接 + #genre# 分组，
                         同一频道多个地址=多行同名，播放器里按"确定"键换源）

用法：
  python fetch_tv_m3u8.py                  采集 + 检测可用性（推荐，需几分钟）
  python fetch_tv_m3u8.py --no-check       只采集不检测，几秒完成
  python fetch_tv_m3u8.py --workers 128 --timeout 5
  python fetch_tv_m3u8.py --max-per-channel 12
  python fetch_tv_m3u8.py --keep-dead      检测后仍保留失效地址（标记保留）
"""

import argparse
import concurrent.futures
import gzip
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.request
import zlib
from html import unescape
from collections import OrderedDict
from pathlib import Path
from urllib.parse import quote, urljoin

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

# 直播源聚合列表。每项：(名称, [可用地址列表，按优先级排列，前一个失败自动换下一个])
# 这些列表由维护者持续更新，若某个长期失效可自行增删。
SOURCES = [
    ("iptv-org中国频道", [
        "https://cdn.jsdelivr.net/gh/iptv-org/iptv@master/streams/cn.m3u",
        "https://iptv-org.github.io/iptv/countries/cn.m3u",
    ]),
    ("epg.pw直播源表", [
        "https://ygas.github.io/iptv-sources/txt/cn.txt",
    ]),
    ("Guovin自动更新", [
        "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.m3u",
        "https://cdn.jsdelivr.net/gh/Guovin/iptv-api@gd/output/result.m3u",
    ]),
    ("YanG聚合源", [
        "https://cdn.jsdelivr.net/gh/YanG-1989/m3u@main/Gather.m3u",
        "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
    ]),
    ("天光云影", [
        "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/iptv4.m3u",
        "https://cdn.jsdelivr.net/gh/vbskycn/iptv@master/tv/iptv4.m3u",
    ]),
    ("直播电视ZBDS", [
        "https://live.zbds.top/tv/iptv4.txt",
        "https://live.zbds.top/tv/iptv4.m3u",
    ]),
    ("咖啡伴侣Live", [
        "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
        "https://cdn.jsdelivr.net/gh/YueChan/Live@main/IPTV.m3u",
    ]),
    ("玩转TV", [
        "https://raw.githubusercontent.com/wwb521/live/main/tv.m3u",
        "https://raw.githubusercontent.com/wwb521/live/main/tv.txt",
        "https://raw.githubusercontent.com/wwb521/live/main/zhibo.txt",
    ]),
    ("fanmingming直播源", [
        "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv6.m3u",
        "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/tv.m3u",
        "https://cdn.jsdelivr.net/gh/fanmingming/live@main/tv/m3u/ipv6.m3u",
    ]),
    ("tvbox-autolive直播源", [
        "https://raw.githubusercontent.com/jwqsir2-lang/tvbox-autolive/main/tvbox.json",
        "https://cdn.jsdelivr.net/gh/jwqsir2-lang/tvbox-autolive@main/tvbox.json",
    ]),
]

# 好趣网是频道目录页，不是现成列表。脚本会抓取目录页，再从每个频道页提取公开的
# m3u8 / m3u / flv / mp4 播放地址；站点若只嵌入网页播放器则自动跳过。
HAOQU_ROOT = "http://tv.haoqu99.com/"
HAOQU_REGION_URLS = {
    "央视": "http://tv.haoqu99.com/1/",
    "卫视": "http://tv.haoqu99.com/2/",
    "北京": "http://tv.haoqu99.com/3/beijing/",
    "天津": "http://tv.haoqu99.com/3/tianjin/",
    "河北": "http://tv.haoqu99.com/3/hebei/",
    "山西": "http://tv.haoqu99.com/3/shan-xi/",
    "内蒙古": "http://tv.haoqu99.com/3/neimenggu/",
    "辽宁": "http://tv.haoqu99.com/3/liaoning/",
    "吉林": "http://tv.haoqu99.com/3/jilin/",
    "黑龙江": "http://tv.haoqu99.com/3/heilongjiang/",
    "上海": "http://tv.haoqu99.com/3/shanghai/",
    "江苏": "http://tv.haoqu99.com/3/jiangsu/",
    "浙江": "http://tv.haoqu99.com/3/zhejiang/",
    "安徽": "http://tv.haoqu99.com/3/anhui/",
    "福建": "http://tv.haoqu99.com/3/fujian/",
    "江西": "http://tv.haoqu99.com/3/jiangxi/",
    "山东": "http://tv.haoqu99.com/3/shandong/",
    "河南": "http://tv.haoqu99.com/3/henan/",
    "湖北": "http://tv.haoqu99.com/3/hubei/",
    "湖南": "http://tv.haoqu99.com/3/hunan/",
    "广东": "http://tv.haoqu99.com/3/guangdong/",
    "广西": "http://tv.haoqu99.com/3/guangxi/",
    "海南": "http://tv.haoqu99.com/3/hainan/",
    "重庆": "http://tv.haoqu99.com/3/zhongqing/",
    "四川": "http://tv.haoqu99.com/3/sichuan/",
    "贵州": "http://tv.haoqu99.com/3/guizhou/",
    "云南": "http://tv.haoqu99.com/3/yunnan/",
    "西藏": "http://tv.haoqu99.com/3/xizang/",
    "陕西": "http://tv.haoqu99.com/3/shanxi/",
    "甘肃": "http://tv.haoqu99.com/3/gansu/",
    "青海": "http://tv.haoqu99.com/3/qinghai/",
    "宁夏": "http://tv.haoqu99.com/3/ningxia/",
    "新疆": "http://tv.haoqu99.com/3/xinjiang/",
}

GROUP_ORDER = ["央视频道", "卫视频道"] + list(HAOQU_REGION_URLS.keys())[2:] + ["港澳台/国际", "其他频道"]

# 部分直播服务器证书不规范，跳过证书校验（只读公开数据，风险可忽略）
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

CCTV_ALIAS = {
    "CCTV1综合": "CCTV1", "CCTV2财经": "CCTV2", "CCTV3综艺": "CCTV3",
    "CCTV4中文国际": "CCTV4", "CCTV4欧洲": "CCTV4", "CCTV4美洲": "CCTV4",
    "CCTV5体育": "CCTV5", "CCTV5+体育赛事": "CCTV5+", "CCTV5PLUS": "CCTV5+",
    "CCTV6电影": "CCTV6", "CCTV7国防军事": "CCTV7", "CCTV7军事农业": "CCTV7",
    "CCTV8电视剧": "CCTV8", "CCTV9纪录": "CCTV9", "CCTV10科教": "CCTV10",
    "CCTV11戏曲": "CCTV11", "CCTV12社会与法": "CCTV12", "CCTV13新闻": "CCTV13",
    "CCTV14少儿": "CCTV14", "CCTV15音乐": "CCTV15", "CCTV16奥林匹克": "CCTV16",
    "CCTV17农业农村": "CCTV17", "CCTV17农业": "CCTV17",
}

REGION_NAMES = ("北京", "天津", "河北", "山西", "内蒙古", "辽宁", "吉林", "黑龙江",
                "上海", "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南",
                "湖北", "湖南", "广东", "广西", "海南", "重庆", "四川", "贵州",
                "云南", "西藏", "陕西", "甘肃", "青海", "宁夏", "新疆", "香港",
                "澳门", "台湾")

MO_TW_HK_INTL = ("香港", "澳门", "澳門", "台湾", "臺灣", "TVB", "凤凰", "鳳凰",
                 "翡翠", "明珠", "星空", "华娱", "華娛", "八大", "中视", "中視",
                 "台视", "台視", "民视", "民視", "三立", "东森", "東森", "中天",
                 "公视", "公視", "龙华", "龍華", "NHK", "BBC", "CNN", "HBO",
                 "国家地理", "探索", "DISCOVERY", "NATIONAL", "KBS", "MBC",
                 "ARIRANG", "TV5", "DW", "FRANCE24",
                 "台北", "新北", "桃园", "台中", "臺中", "台南", "臺南", "高雄",
                 "基隆", "新竹", "嘉义", "嘉義", "屏东", "屏東", "花莲", "花蓮")

# 地级市、区县级地方台对应省份：让市级/区县级频道也归入各自省份分组。
# 覆盖范围以常用地级市为主，兼顾常见出现在台名中的区县/县级市。
CITY_TO_REGION = {
    # 河北
    "石家庄": "河北", "唐山": "河北", "秦皇岛": "河北", "邯郸": "河北", "邢台": "河北",
    "保定": "河北", "张家口": "河北", "承德": "河北", "沧州": "河北", "廊坊": "河北",
    "衡水": "河北", "辛集": "河北", "定州": "河北", "雄安": "河北",
    # 山西
    "太原": "山西", "大同": "山西", "阳泉": "山西", "长治": "山西", "晋城": "山西",
    "朔州": "山西", "晋中": "山西", "运城": "山西", "忻州": "山西", "临汾": "山西",
    "吕梁": "山西",
    # 内蒙古
    "呼和浩特": "内蒙古", "包头": "内蒙古", "乌海": "内蒙古", "赤峰": "内蒙古",
    "通辽": "内蒙古", "鄂尔多斯": "内蒙古", "呼伦贝尔": "内蒙古", "巴彦淖尔": "内蒙古",
    "乌兰察布": "内蒙古", "兴安盟": "内蒙古", "锡林郭勒": "内蒙古", "阿拉善": "内蒙古",
    # 辽宁
    "沈阳": "辽宁", "大连": "辽宁", "鞍山": "辽宁", "抚顺": "辽宁", "本溪": "辽宁",
    "丹东": "辽宁", "锦州": "辽宁", "营口": "辽宁", "阜新": "辽宁", "辽阳": "辽宁",
    "盘锦": "辽宁", "铁岭": "辽宁", "朝阳": "辽宁", "葫芦岛": "辽宁",
    # 吉林
    "长春": "吉林", "四平": "吉林", "辽源": "吉林", "通化": "吉林", "白山": "吉林",
    "松原": "吉林", "白城": "吉林", "延边": "吉林", "延吉": "吉林",
    # 黑龙江
    "哈尔滨": "黑龙江", "齐齐哈尔": "黑龙江", "鸡西": "黑龙江", "鹤岗": "黑龙江",
    "双鸭山": "黑龙江", "大庆": "黑龙江", "伊春": "黑龙江", "佳木斯": "黑龙江",
    "七台河": "黑龙江", "牡丹江": "黑龙江", "黑河": "黑龙江", "绥化": "黑龙江",
    "大兴安岭": "黑龙江",
    # 江苏
    "南京": "江苏", "无锡": "江苏", "徐州": "江苏", "常州": "江苏", "苏州": "江苏",
    "南通": "江苏", "连云港": "江苏", "淮安": "江苏", "盐城": "江苏", "扬州": "江苏",
    "镇江": "江苏", "泰州": "江苏", "宿迁": "江苏", "昆山": "江苏", "常熟": "江苏",
    "张家港": "江苏", "江阴": "江苏",
    # 浙江
    "杭州": "浙江", "宁波": "浙江", "温州": "浙江", "嘉兴": "浙江", "湖州": "浙江",
    "绍兴": "浙江", "金华": "浙江", "衢州": "浙江", "舟山": "浙江", "台州": "浙江",
    "丽水": "浙江", "义乌": "浙江", "慈溪": "浙江", "余姚": "浙江", "萧山": "浙江",
    # 安徽
    "合肥": "安徽", "芜湖": "安徽", "蚌埠": "安徽", "淮南": "安徽", "马鞍山": "安徽",
    "淮北": "安徽", "铜陵": "安徽", "安庆": "安徽", "黄山": "安徽", "滁州": "安徽",
    "阜阳": "安徽", "宿州": "安徽", "六安": "安徽", "亳州": "安徽", "池州": "安徽",
    "宣城": "安徽",
    # 福建
    "福州": "福建", "厦门": "福建", "莆田": "福建", "三明": "福建", "泉州": "福建",
    "漳州": "福建", "南平": "福建", "龙岩": "福建", "宁德": "福建",
    # 江西
    "南昌": "江西", "景德镇": "江西", "萍乡": "江西", "九江": "江西", "新余": "江西",
    "鹰潭": "江西", "赣州": "江西", "吉安": "江西", "宜春": "江西", "抚州": "江西",
    "上饶": "江西",
    # 山东
    "济南": "山东", "青岛": "山东", "淄博": "山东", "枣庄": "山东", "东营": "山东",
    "烟台": "山东", "潍坊": "山东", "济宁": "山东", "泰安": "山东", "威海": "山东",
    "日照": "山东", "临沂": "山东", "德州": "山东", "聊城": "山东", "滨州": "山东",
    "菏泽": "山东",
    # 河南
    "郑州": "河南", "开封": "河南", "洛阳": "河南", "平顶山": "河南", "安阳": "河南",
    "鹤壁": "河南", "新乡": "河南", "焦作": "河南", "濮阳": "河南", "许昌": "河南",
    "漯河": "河南", "三门峡": "河南", "南阳": "河南", "商丘": "河南", "信阳": "河南",
    "周口": "河南", "驻马店": "河南", "济源": "河南",
    # 湖北
    "武汉": "湖北", "黄石": "湖北", "十堰": "湖北", "宜昌": "湖北", "襄阳": "湖北",
    "鄂州": "湖北", "荆门": "湖北", "孝感": "湖北", "荆州": "湖北", "黄冈": "湖北",
    "咸宁": "湖北", "随州": "湖北", "恩施": "湖北", "仙桃": "湖北", "潜江": "湖北",
    "天门": "湖北",
    # 湖南
    "长沙": "湖南", "株洲": "湖南", "湘潭": "湖南", "衡阳": "湖南", "邵阳": "湖南",
    "岳阳": "湖南", "常德": "湖南", "张家界": "湖南", "益阳": "湖南", "郴州": "湖南",
    "永州": "湖南", "怀化": "湖南", "娄底": "湖南", "湘西": "湖南",
    # 广东
    "广州": "广东", "韶关": "广东", "深圳": "广东", "珠海": "广东", "汕头": "广东",
    "佛山": "广东", "江门": "广东", "湛江": "广东", "茂名": "广东", "肇庆": "广东",
    "惠州": "广东", "梅州": "广东", "汕尾": "广东", "河源": "广东", "阳江": "广东",
    "清远": "广东", "东莞": "广东", "中山": "广东", "潮州": "广东", "揭阳": "广东",
    "云浮": "广东", "顺德": "广东", "南海": "广东", "番禺": "广东", "增城": "广东",
    "从化": "广东", "花都": "广东",
    # 广西
    "南宁": "广西", "柳州": "广西", "桂林": "广西", "梧州": "广西", "北海": "广西",
    "防城港": "广西", "钦州": "广西", "贵港": "广西", "玉林": "广西", "百色": "广西",
    "贺州": "广西", "河池": "广西", "来宾": "广西", "崇左": "广西",
    # 海南
    "海口": "海南", "三亚": "海南", "三沙": "海南", "儋州": "海南", "五指山": "海南",
    "琼海": "海南", "文昌": "海南", "万宁": "海南", "东方": "海南",
    # 四川
    "成都": "四川", "自贡": "四川", "攀枝花": "四川", "泸州": "四川", "德阳": "四川",
    "绵阳": "四川", "广元": "四川", "遂宁": "四川", "内江": "四川", "乐山": "四川",
    "南充": "四川", "眉山": "四川", "宜宾": "四川", "广安": "四川", "达州": "四川",
    "雅安": "四川", "巴中": "四川", "资阳": "四川", "西昌": "四川", "凉山": "四川",
    "甘孜": "四川", "阿坝": "四川", "天府": "四川",
    # 贵州
    "贵阳": "贵州", "六盘水": "贵州", "遵义": "贵州", "安顺": "贵州", "毕节": "贵州",
    "铜仁": "贵州", "黔东南": "贵州", "黔南": "贵州", "黔西南": "贵州",
    # 云南
    "昆明": "云南", "曲靖": "云南", "玉溪": "云南", "保山": "云南", "昭通": "云南",
    "丽江": "云南", "普洱": "云南", "临沧": "云南", "楚雄": "云南", "红河": "云南",
    "文山": "云南", "西双版纳": "云南", "大理": "云南", "德宏": "云南", "怒江": "云南",
    "迪庆": "云南",
    # 西藏
    "拉萨": "西藏", "日喀则": "西藏", "昌都": "西藏", "林芝": "西藏", "山南": "西藏",
    "那曲": "西藏", "阿里": "西藏",
    # 陕西
    "西安": "陕西", "铜川": "陕西", "宝鸡": "陕西", "咸阳": "陕西", "渭南": "陕西",
    "延安": "陕西", "汉中": "陕西", "榆林": "陕西", "安康": "陕西", "商洛": "陕西",
    # 甘肃
    "兰州": "甘肃", "嘉峪关": "甘肃", "金昌": "甘肃", "白银": "甘肃", "天水": "甘肃",
    "武威": "甘肃", "张掖": "甘肃", "平凉": "甘肃", "酒泉": "甘肃", "庆阳": "甘肃",
    "定西": "甘肃", "陇南": "甘肃", "临夏": "甘肃", "甘南": "甘肃",
    # 青海
    "西宁": "青海", "海东": "青海", "海北": "青海", "黄南": "青海", "果洛": "青海",
    "玉树": "青海", "海西": "青海",
    # 宁夏
    "银川": "宁夏", "石嘴山": "宁夏", "吴忠": "宁夏", "固原": "宁夏", "中卫": "宁夏",
    # 新疆
    "乌鲁木齐": "新疆", "克拉玛依": "新疆", "吐鲁番": "新疆", "哈密": "新疆",
    "昌吉": "新疆", "博尔塔拉": "新疆", "巴音郭楞": "新疆", "阿克苏": "新疆",
    "克孜勒苏": "新疆", "喀什": "新疆", "和田": "新疆", "伊犁": "新疆", "塔城": "新疆",
    "阿勒泰": "新疆",
}

BRACKET_RE = re.compile(r"[（(【\[][^）)】\]]{0,12}[）)】\]]")
SUFFIXES = ("频道", "电视台", "高清", "超清", "HD", "FHD")
JUNK_NAMES = {"CCTV", "TEST", "测试", "4K", "8K", "VIP", "推荐", "直播", "备用"}


def normalize_name(raw):
    """频道名归一化：去掉 [HD]【高清】 等标记、空白、常见后缀，统一 CCTV 写法，
    使 'CCTV-1综合'、'CCTV1[高清]'、'CCTV1频道' 都归并到同一个频道。"""
    s = BRACKET_RE.sub("", raw.strip())
    s = re.sub(r"\s+", "", s)
    s = s.upper()
    for sep in ("CCTV-", "CCTV－", "CCTV—", "CCTV·"):
        s = s.replace(sep, "CCTV")
    s = CCTV_ALIAS.get(s, s)
    changed = True
    while changed and len(s) > 2:
        changed = False
        for suf in SUFFIXES:
            if s.endswith(suf) and len(s) - len(suf) >= 2:
                s = s[:-len(suf)]
                changed = True
    return s


def classify(name):
    if name.startswith("CCTV") or name.startswith("CGTN"):
        return "央视频道"
    for key in MO_TW_HK_INTL:
        if key in name:
            return "港澳台/国际"
    if "卫视" in name:
        return "卫视频道"
    for region in REGION_NAMES:
        if region in name:
            return region
    # 市级、区县级地方台：按城市归属到对应省份，避免散落在“其他频道”
    for city, region in CITY_TO_REGION.items():
        if city in name:
            return region
    return "其他频道"


def parse_m3u(text):
    """解析 M3U/M3U8 文本，返回 [(频道名, url)]"""
    items = []
    pending = ""
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#EXTINF"):
            pending = line.rsplit(",", 1)[1].strip() if "," in line else ""
        elif line.startswith("#"):
            continue
        elif pending:
            items.append((pending, line))
            pending = ""
    return items


def parse_txt(text):
    """解析 '频道名,链接' 格式（DIYP），返回 [(频道名, url)]"""
    items = []
    for line in text.splitlines():
        line = line.strip().replace("，", ",")
        if not line or line.startswith("#") or "," not in line:
            continue
        name, url = line.rsplit(",", 1)
        name, url = name.strip(), url.strip()
        if url.startswith(("http://", "https://", "rtmp://", "rtp://", "udp://")):
            items.append((name, url))
    return items


def parse_tvbox_json(text):
    """解析 TVBox 配置里的 lives 直播列表。

    返回 ([(频道名, url)], [待展开的列表地址])。
    urls 字段的地址末尾常带 $线路N 后缀，去掉后才是真正的播放地址。
    """
    try:
        obj = json.loads(text)
    except (ValueError, TypeError):
        return [], []
    items, sub_lists = [], []
    for live in obj.get("lives") or []:
        if not isinstance(live, dict):
            continue
        for ch in live.get("channels") or []:
            if not isinstance(ch, dict):
                continue
            name = str(ch.get("name") or "").strip()
            for u in ch.get("urls") or []:
                u = str(u).split("$")[0].strip()
                if name and u.startswith(("http://", "https://")):
                    items.append((name, u))
        # 有些配置的 lives 项直接指向一个 m3u/txt 列表文件，展开一层
        u = str(live.get("url") or "").strip()
        if re.search(r"\.(?:m3u|txt|json)(?:$|\?)", u, re.I):
            sub_lists.append(u)
    return items, sub_lists


def fetch_nested_lists(list_urls, timeout):
    """展开 TVBox 配置里 lives 指向的 m3u/txt 列表文件（只展开一层）。"""
    items = []
    for u in list_urls:
        _, data = http_get(u, timeout=timeout)
        if not data:
            continue
        text = data.decode("utf-8", "ignore")
        items.extend(parse_m3u(text) if "#EXTINF" in text else parse_txt(text))
    return items


def parse_haoqu_page(text, region):
    """从好趣网频道目录/详情页提取公开媒体地址。返回 [(频道名, url)]。"""
    text = unescape(text).replace("\\/", "/")
    links = re.findall(r"https?://[^\"'<>\s]+", text, flags=re.I)
    media_re = re.compile(r"\.(?:m3u8?|flv|mp4)(?:\?[^\"'<>\s]*)?$", re.I)
    result = []
    for link in links:
        link = link.rstrip("\),;'")
        if media_re.search(link):
            result.append((region, link))
    return result


def extract_haoqu_channels(text, region_url):
    """提取好趣网页中的频道名称和详情页链接，避免把分类/图片链接当频道。"""
    result = []
    pattern = re.compile(r'<a[^>]+href=["\']([^"\']+\.html)["\'][^>]*>(.*?)</a>', re.I | re.S)
    for href, label in pattern.findall(text):
        label = re.sub(r"<[^>]+>", "", unescape(label)).strip()
        label = re.sub(r"\s+", "", label)
        if not label or len(label) > 40 or "在线直播" in label:
            continue
        if href.startswith("/"):
            href = HAOQU_ROOT.rstrip("/") + href
        elif not href.startswith("http"):
            href = region_url.rstrip("/") + "/" + href.lstrip("/")
        result.append((label, href))
    return list(OrderedDict(((n, u), True) for n, u in result).keys())


def safe_url(url):
    """转义 URL 里的非 ASCII 字符（中文路径等），避免 urllib 抛 UnicodeEncodeError。"""
    try:
        return quote(url, safe="%:/?#[]@!$&'()*+,;=~")
    except Exception:
        return url


def http_get_final(url, timeout=10, max_bytes=None):
    """请求 URL 并跟随重定向，返回 (状态码, 数据, 最终地址)。

    最终地址很关键：不少 IPTV 源的 m3u8 会 302 跳到另一台服务器，
    分片相对路径必须以跳转后的地址为基准解析，否则会全部取到 404。
    """
    req = urllib.request.Request(safe_url(url), headers={
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Encoding": "gzip",
        "Connection": "close",
    })
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            code = resp.getcode()
            final = resp.geturl() or url
            data = resp.read(max_bytes) if max_bytes else resp.read()
            if "gzip" in (resp.headers.get("Content-Encoding") or "").lower():
                try:
                    data = gzip.decompress(data)
                except (OSError, EOFError, zlib.error):
                    pass  # 截断的 gzip，保留原始字节做粗略判断
            return code, data, final
    except urllib.error.HTTPError as exc:
        # HTTPError 也可能携带响应体，但错误码必须视为不可用，交给调用方剔除。
        return exc.code, None, getattr(exc, "url", None) or url
    except Exception:
        return None, None, url


def http_get(url, timeout=10, max_bytes=None):
    code, data, _ = http_get_final(url, timeout, max_bytes)
    return code, data


def fetch_source(label, urls, timeout=25):
    # 国内访问 github/jsdelivr 时通时断：全部地址轮询三轮，轮次间歇 3 秒对抗抖动
    for attempt, url in enumerate(urls * 3):
        if attempt and attempt % len(urls) == 0:
            time.sleep(3)
        code, data = http_get(url, timeout=timeout)
        if not data:
            continue
        head = data[:2048].decode("utf-8", "ignore").lower()
        if head.startswith("<!doctype") or "<html" in head:
            continue  # 拿到的是网页而不是列表
        text = data.decode("utf-8", "ignore")
        if text.lstrip().startswith("{") and '"lives"' in text:
            items, sub_lists = parse_tvbox_json(text)
            items.extend(fetch_nested_lists(sub_lists, timeout))
        elif "#EXTINF" in text:
            items = parse_m3u(text)
        else:
            items = parse_txt(text)
        if items:
            return label, url, items
    return label, None, []


def fetch_haoqu_source(timeout=15, max_pages=800):
    """抓取好趣网目录及频道详情页中的公开媒体直链。"""
    pages = []
    for region, index_url in HAOQU_REGION_URLS.items():
        _, data = http_get(index_url, timeout=timeout)
        if not data:
            continue
        text = data.decode("utf-8", "ignore")
        for channel_name, detail_url in extract_haoqu_channels(text, index_url):
            pages.append((region, channel_name, detail_url))
    unique = list(OrderedDict(((r, n, u), True) for r, n, u in pages).keys())[:max_pages]
    items = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as ex:
        futures = {ex.submit(http_get, u, timeout): (region, name) for region, name, u in unique}
        for fut in concurrent.futures.as_completed(futures):
            region, name = futures[fut]
            try:
                _, data = fut.result()
            except Exception:
                data = None
            if data:
                items.extend((name, url) for _, url in parse_haoqu_page(
                    data.decode("utf-8", "ignore"), region))
    return "好趣网目录", HAOQU_ROOT, items


def pick_media_urls(playlist_text, base_url, timeout, depth=0):
    """返回可用于测速的媒体地址候选（按优先级排序）。

    主播放列表（含 #EXT-X-STREAM-INF）里的地址是清晰度变体，先下钻到子列表；
    媒体播放列表里同时给出最新分片和最早分片——直播流的旧分片常已被服务器
    淘汰，只取第一个会被误判成死链。
    """
    lines = [ln.strip() for ln in playlist_text.splitlines() if ln.strip()]
    urls = [ln for ln in lines if not ln.startswith("#")]
    if not urls:
        return []
    if any(ln.upper().startswith("#EXT-X-STREAM-INF") for ln in lines):
        if depth >= 2:
            return []
        variant = urljoin(base_url, urls[0])
        _, data, final = http_get_final(variant, timeout=timeout, max_bytes=32768)
        if not data:
            return []
        return pick_media_urls(data.decode("utf-8", "ignore"), final or variant,
                               timeout, depth + 1)
    candidates = [urljoin(base_url, urls[-1])]
    first = urljoin(base_url, urls[0])
    if first not in candidates:
        candidates.append(first)
    return candidates


PLACEHOLDER_MARKERS = ("NOSIGNAL", "NO-SIGNAL", "NO_SIGNAL", "无信号", "NOVIDEO",
                       "NO_VIDEO", "BLACKSCREEN", "BLACK_SCREEN", "PLACEHOLDER",
                       "TESTCARD", "COLORBAR", "COLOR_BAR")

# 检测结果分级：这类源的分片时有时无（间歇性），一次性检测只是快照，
# 所以把"列表正常但当下没取到分片"的降级保留为备用，而不是直接删掉。
STATE_OK = "ok"      # 列表正常且分片可取，确认可播
STATE_WEAK = "weak"  # 列表正常但分片未取到，降级为备用
STATE_DEAD = "dead"  # 列表本身有问题（错误码/网页/占位流/空列表），删除


def is_placeholder(*texts):
    """识别"无信号"之类的占位流：这类地址能返回 200，但画面是空的。"""
    for t in texts:
        up = t.upper()
        if any(mk in up for mk in PLACEHOLDER_MARKERS):
            return True
    return False


def looks_like_media(data):
    """判断响应体是不是音视频数据：TS 以 0x47 同步字节开头，FLV/MP4 有各自文件头。"""
    if not data:
        return False
    if data[0] == 0x47:
        return True
    if data[:3] == b"FLV":
        return True
    return b"ftyp" in data[:64]


def fetch_sample(url, timeout, sample_bytes, sample_seconds):
    """限时下载一小段数据测速，返回 (状态码, 字节数, 耗时秒, 是否HTML页面)。"""
    req = urllib.request.Request(safe_url(url), headers={
        "User-Agent": UA, "Accept": "*/*",
        "Accept-Encoding": "identity", "Connection": "close",
    })
    got, t0 = 0, time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
            code = resp.getcode()
            if code is None or not 200 <= code < 300:
                return code, 0, 0.0, False
            first = resp.read(2048)
            got += len(first)
            if looks_like_html(first.decode("utf-8", "ignore")):
                return code, got, time.time() - t0, True
            while got < sample_bytes and time.time() - t0 < sample_seconds:
                chunk = resp.read(min(16384, sample_bytes - got))
                if not chunk:
                    break
                got += len(chunk)
            return code, got, time.time() - t0, False
    except urllib.error.HTTPError as exc:
        return exc.code, got, time.time() - t0, False
    except Exception:
        return None, got, time.time() - t0, False


def looks_like_html(text):
    """判断响应内容是不是网页（错误提示页/登录页等），而不是播放列表或媒体数据。"""
    head = text[:2048].lstrip("\ufeff \t\r\n").upper()
    if head.startswith("<!DOCTYPE") or head.startswith("<?XML"):
        return True
    return any(tag in head for tag in ("<HTML", "<HEAD", "<BODY", "<SCRIPT", "<TITLE"))


def probe_url(url, timeout, sample_bytes=65536, sample_seconds=3.0):
    """检测可用性并实测下载速度，返回 (state, code, speed_kbps, latency_ms)。

    state 取值见 STATE_OK / STATE_WEAK / STATE_DEAD：
      - 非 2xx、连接失败、网页错误页、无信号占位流、空播放列表  -> STATE_DEAD
      - 列表正常但当下没取到分片（间歇性源常见）               -> STATE_WEAK
      - 列表正常且分片可取                                     -> STATE_OK
    速度取自分片或直链的实际下载速率，用于同一频道内按流畅度排序。
    """
    if is_placeholder(url):
        return STATE_DEAD, 200, 0.0, 0

    req = urllib.request.Request(safe_url(url), headers={
        "User-Agent": UA, "Accept": "*/*",
        "Accept-Encoding": "identity", "Connection": "close",
    })
    t0 = time.time()
    code = ctype = body = None
    final_url = url  # 跟随重定向后的最终地址，分片相对路径要以它为基准（VLC 同此）
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
                code = resp.getcode()
                ctype = (resp.headers.get("Content-Type") or "").lower()
                final_url = resp.geturl() or url
                body = resp.read(32768)
            break
        except urllib.error.HTTPError as exc:
            return STATE_DEAD, exc.code, 0.0, 0
        except Exception:
            if attempt == 0:
                time.sleep(0.6)  # 网络抖动，重试一次再判定
                continue
            return STATE_DEAD, None, 0.0, 0

    latency = int((time.time() - t0) * 1000)
    if code is None or not 200 <= code < 300 or not body:
        return STATE_DEAD, code, 0.0, latency

    text = body.decode("utf-8", "ignore")
    head = text[:4096].upper()
    is_playlist = "#EXTM3U" in head or "#EXT-X" in head

    if not is_playlist:
        # 没有播放列表特征：网页一律删除；音视频数据或 .m3u8 地址保留
        if "text/html" in ctype or looks_like_html(text):
            return STATE_DEAD, code, 0.0, latency
        if looks_like_media(body) or ".M3U8" in url.upper() or "MPEGURL" in head:
            speed = len(body) / max(time.time() - t0, 0.05) / 1024
            return STATE_OK, code, speed, latency
        return STATE_DEAD, code, 0.0, latency

    if is_placeholder(text):
        return STATE_DEAD, code, 0.0, latency

    candidates = pick_media_urls(text, final_url, timeout)
    if not candidates:
        return STATE_DEAD, code, 0.0, latency  # 空播放列表，没有可用分片

    last_code = None
    for cand in candidates:
        if is_placeholder(cand):
            return STATE_DEAD, code, 0.0, latency  # 无信号占位流
        seg_code, seg_bytes, seg_secs, seg_html = fetch_sample(
            cand, timeout, sample_bytes, sample_seconds)
        if seg_html:
            continue
        if seg_code is not None and not 200 <= seg_code < 300:
            last_code = seg_code
            continue
        speed = seg_bytes / seg_secs / 1024 if seg_secs > 0 else 0.0
        return STATE_OK, code, speed, latency

    return STATE_WEAK, last_code if last_code is not None else code, 0.0, latency


def natural_key(name):
    return re.sub(r"\d+", lambda m: m.group(0).zfill(4), name)


def main():
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    ap = argparse.ArgumentParser(description="全国电视台直播源采集")
    ap.add_argument("--no-check", action="store_true", help="不检测可用性（速度快）")
    ap.add_argument("--timeout", type=int, default=6, help="单地址检测超时秒数（默认6）")
    ap.add_argument("--workers", type=int, default=80, help="并发检测线程数（默认80）")
    ap.add_argument("--max-per-channel", type=int, default=8,
                    help="每个频道最多保留的地址数（默认8）")
    ap.add_argument("--keep-dead", action="store_true",
                    help="检测后仍保留失效地址")
    ap.add_argument("--strict", action="store_true",
                    help="只保留确认可播的地址，丢弃降级备用（默认保留备用）")
    ap.add_argument("--out-prefix", default="tv_channels", help="输出文件名前缀")
    args = ap.parse_args()

    print("=" * 56)
    print(" 全国电视台直播源采集")
    print("=" * 56)

    print("\n[1/3] 抓取直播源聚合列表...")
    all_items = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futs = [ex.submit(fetch_source, label, urls) for label, urls in SOURCES]
        for fut in concurrent.futures.as_completed(futs):
            label, used_url, items = fut.result()
            if items:
                print("  [OK] %s: %d 条" % (label, len(items)))
                all_items.extend(items)
            else:
                print("  [失败] %s: 所有地址均不可用" % label)
    print("  [..] 抓取好趣网目录频道页...")
    try:
        label, used_url, items = fetch_haoqu_source()
        if items:
            print("  [OK] %s: %d 条" % (label, len(items)))
            all_items.extend(items)
        else:
            print("  [提示] 好趣网没有提取到公开媒体直链")
    except Exception as exc:
        print("  [失败] 好趣网目录：%s" % exc)
    if not all_items:
        print("\n所有聚合源都抓取失败，请检查网络后重试。")
        return 1

    print("\n[2/3] 合并频道（同名归并、去重）...")
    channels = OrderedDict()
    for name, url in all_items:
        url = url.strip()
        n = normalize_name(name)
        if not url.startswith(("http://", "https://")):
            continue  # 只保留可通用播放的 http/https(m3u8) 地址，丢弃组播/rtmp
        if not n or n in JUNK_NAMES or len(n) > 24:
            continue
        channels.setdefault(n, OrderedDict())[url] = True
    unique_urls = [u for urls in channels.values() for u in urls]
    print("  共 %d 个频道，%d 条唯一地址" % (len(channels), len(unique_urls)))

    keep = None  # None = 全部保留
    speeds = {}  # url -> 实测下载速度 KB/s，用于同一频道内按流畅度排序
    verified = set()  # 确认可播（列表正常且分片可取）的地址
    if not args.no_check:
        print("\n[3/3] 检测可用性并测速（超时 %ds，并发 %d）..." % (args.timeout, args.workers))
        alive, weak, dead = set(), set(), set()
        done = 0
        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = {ex.submit(probe_url, u, args.timeout): u for u in unique_urls}
            for fut in concurrent.futures.as_completed(futs):
                u = futs[fut]
                try:
                    state, code, speed, latency = fut.result()
                except Exception:
                    state, speed = STATE_DEAD, 0.0
                if state == STATE_OK:
                    alive.add(u)
                    verified.add(u)
                    speeds[u] = speed
                elif state == STATE_WEAK:
                    weak.add(u)
                else:
                    dead.add(u)
                done += 1
                if done % 200 == 0 or done == len(unique_urls):
                    rate = done / max(time.time() - t0, 0.1)
                    print("  已检测 %d/%d（%.0f 条/秒），确认可播 %d，降级备用 %d"
                          % (done, len(unique_urls), rate, len(alive), len(weak)))
        if args.keep_dead:
            keep = None
        elif args.strict:
            keep = alive
        else:
            keep = alive | weak
        print("  确认可播 %d，降级备用 %d，已删除 %d / 共 %d"
              % (len(alive), len(weak), len(dead), len(unique_urls)))
    else:
        print("\n[3/3] 跳过检测（--no-check）")

    result = OrderedDict()
    for name, urls in channels.items():
        keep_urls = [u for u in urls if keep is None or u in keep]
        # 先排确认可播的，再排降级备用的；同档内按实测速度从高到低
        keep_urls.sort(key=lambda u: (u in verified, speeds.get(u, 0.0)), reverse=True)
        if keep_urls:
            result[name] = keep_urls[:args.max_per_channel]
    if not result:
        print("\n检测后没有可用频道。可尝试 --no-check 或增大 --timeout 后重试。")
        return 1

    out_dir = Path(__file__).resolve().parent
    m3u_path = out_dir / (args.out_prefix + ".m3u")
    txt_path = out_dir / (args.out_prefix + ".txt")

    m3u = ["#EXTM3U"]
    txt = []
    for group in GROUP_ORDER:
        names = sorted((n for n in result if classify(n) == group), key=natural_key)
        if not names:
            continue
        txt.append("%s,#genre#" % group)
        for n in names:
            for u in result[n]:
                m3u.append('#EXTINF:-1 group-title="%s",%s' % (group, n))
                m3u.append(u)
                txt.append("%s,%s" % (n, u))

    m3u_path.write_text("\n".join(m3u) + "\n", encoding="utf-8")
    txt_path.write_text("\n".join(txt) + "\n", encoding="utf-8")

    total_links = sum(len(v) for v in result.values())
    print("\n" + "=" * 56)
    print(" 采集完成！共 %d 个频道 / %d 条地址" % (len(result), total_links))
    for group in GROUP_ORDER:
        cnt = sum(1 for n in result if classify(n) == group)
        links = sum(len(result[n]) for n in result if classify(n) == group)
        print("   %-8s %4d 个频道 / %5d 条地址" % (group, cnt, links))
    multi = sum(1 for v in result.values() if len(v) >= 2)
    print("   其中 %d 个频道有 2 个以上备用地址" % multi)
    if not args.no_check:
        v1 = sum(1 for v in result.values() for u in v if u in verified)
        print("   确认可播 %d 条，降级备用 %d 条（同频道内按此顺序排列）"
              % (v1, total_links - v1))
    print("\n 已保存：")
    print("   %s" % m3u_path)
    print("   %s" % txt_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
