from __future__ import annotations

import torch
import torchvision
from PIL import Image
from ultralytics import YOLO
import ray
import time

import daft
from daft.expressions import col

start = time.perf_counter()
# 由于下方模型UDF处gpus设置为0，所以此时该参数用于设置一次起多少个基于CPU的解析任务，由于mock了模型可以设置的大些
NUM_GPU_NODES = 32
# 需要自行下载模型，并修改路径为自己的模型路径，避免网络影响
YOLO_MODEL = "yolo11n.pt"
# 数据输入路径，需要修改成本地路径
INPUT_PATH = (
    "/home/data/video_benchmark"
)
# 输出路径，需要修改成本地路径
OUTPUT_PATH = "/home/data/video_benchmark/output"
IMAGE_HEIGHT = 640
IMAGE_WIDTH = 640

# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass
ray.get([warmup.remote() for _ in range(64)])

# 取消掉GPU设置
@daft.cls(
    max_concurrency=NUM_GPU_NODES,
    gpus=0,
)
class ExtractImageFeatures:
    def __init__(self):
        self.model = None

    @daft.method.batch(
        return_dtype=daft.DataType.list(
            daft.DataType.struct(
                {
                    "label": daft.DataType.string(),
                    "confidence": daft.DataType.float32(),
                    "bbox": daft.DataType.list(daft.DataType.int32()),
                }
            )
        )
    )
    def __call__(self, images):
        # 注释模型处理
        batch_len = len(images)
        if batch_len == 0:
            return []

        # 伪造检测结果
        mock_detection = [{
            "label": "stub_object",
            "confidence": 0.95,
            "bbox": [0, 0, 100, 100],  # 伪造一个 100x100 的检测框
        }]

        # 为批次中的每一帧返回相同的伪造结果
        return daft.Series.from_pylist([mock_detection for _ in range(batch_len)])


daft.set_runner_ray()

# 本地 IO，取消掉s3配置，避免报错
daft.set_planning_config(default_io_config=daft.io.IOConfig())

start_time = time.time()

df = daft.read_video_frames(
    INPUT_PATH,
    image_height=IMAGE_HEIGHT,
    image_width=IMAGE_WIDTH,
)
df = df.with_column("features", ExtractImageFeatures()(col("data")))
df = df.explode("features")
df = df.with_column("object", daft.col("data").crop(daft.col("features")["bbox"]).encode_image("png"))
df = df.exclude("data")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
end = time.perf_counter() - start
print(f"ALL PYTHON Time taken: {end} seconds")