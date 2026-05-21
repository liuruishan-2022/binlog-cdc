from PIL import Image, ImageDraw, ImageFont


W, H = 1600, 900
FONT = "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
OUT = "docs/unpublished/assets/promtail-vector-comparison.png"


def font(size):
    return ImageFont.truetype(FONT, size)


def rounded(draw, xy, radius, fill, outline=None, width=1):
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def text(draw, xy, content, size, fill="#111827"):
    draw.text(xy, content, font=font(size), fill=fill)


def bullet(draw, x, y, content, size=22):
    text(draw, (x, y), "• " + content, size, "#334155")


img = Image.new("RGB", (W, H), "#f4f7fb")
draw = ImageDraw.Draw(img)

draw.ellipse((1260, -10, 1500, 230), fill="#dbeafe")
draw.ellipse((20, 610, 320, 910), fill="#dcfce7")

text(draw, (96, 66), "Promtail 与 Vector 日志采集对比", 56)
text(draw, (98, 138), "生产新增链路推荐 Vector，基于真实 K8s 部署、本地采集配置与高流量生产数据", 26, "#475569")

cards = [
    (96, 215, 491, 545, "Promtail", "#64748b", "存量", [
        "Loki 生态采集 Agent",
        "现有主日志链路已验证",
        "覆盖 Pod、节点、VM 日志",
    ], ["风险：已 EOL", "multiline / regex 配置需谨慎"]),
    (602, 215, 997, 545, "Vector", "#059669", "推荐", [
        "file source 写入 Loki",
        "OTLP metrics 暴露 Prometheus",
        "source -> sink 模型清晰",
    ], ["新增链路优先", "迁移需验证 label 与 checkpoint"]),
    (1108, 215, 1503, 545, "Loki 3.6.8", "#2563eb", "后端", [
        "接收 Vector 日志写入",
        "Grafana 查询与告警",
        "高峰写入需配套限流调优",
    ], ["地址格式：loki 的地址:端口", "/loki/api/v1/push"]),
]

for x1, y1, x2, y2, title, color, chip, lines, notes in cards:
    rounded(draw, (x1, y1, x2, y2), 18, "#ffffff", "#d9e2ec", 2)
    rounded(draw, (x1 + 30, y1 + 30, x1 + 145, y1 + 64), 17, color)
    text(draw, (x1 + 63, y1 + 33), chip, 18, "#ffffff")
    text(draw, (x1 + 30, y1 + 90), title, 30)
    yy = y1 + 140
    for line in lines:
        bullet(draw, x1 + 30, yy, line)
        yy += 38
    yy = y2 - 68
    for note in notes:
        text(draw, (x1 + 30, yy), note, 18, "#64748b")
        yy += 28

draw.line((508, 380, 582, 380), fill="#475569", width=4)
draw.polygon([(582, 380), (568, 372), (568, 388)], fill="#475569")
draw.line((1014, 380, 1088, 380), fill="#475569", width=4)
draw.polygon([(1088, 380), (1074, 372), (1074, 388)], fill="#475569")

metrics = [
    (132, "2 亿", "生产服务中心日请求量"),
    (478, "70w/s", "日志采集峰值"),
    (824, "140-150 亿", "每日日志行数"),
    (1170, "8m / 134Mi", "当前 logs/vector 实时总用量"),
]

for x, value, label in metrics:
    rounded(draw, (x, 630, x + 300, 768), 18, "#ffffff", "#d9e2ec", 2)
    text(draw, (x + 30, 658), value, 34)
    text(draw, (x + 30, 716), label, 20, "#475569")

text(draw, (96, 836), "结论：新增生产日志采集链路优先使用 Vector；Promtail 存量链路可维护，迁移需重新验证 label、checkpoint、限流与查询习惯。", 20, "#334155")

img.save(OUT)
