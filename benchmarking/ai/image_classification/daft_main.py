from __future__ import annotations

import numpy as np
import torch
from torchvision import transforms
from torchvision.models import ResNet18_Weights, resnet18
import time
import ray

import daft
from daft import col

start = time.perf_counter()
# 由于下方模型UDF处gpus设置为0，所以此时该参数用于设置一次起多少个基于CPU的解析任务，由于mock了模型可以设置的大些
NUM_GPU_NODES = 8
# 数据输入路径，需要修改成本地路径
INPUT_PATH = "/home/data/image_benchmark/parquet"
# 输出路径，需要修改成本地路径
OUTPUT_PATH = "/home/data/image_benchmark/parquet/image_classification_results"
BATCH_SIZE = 100
IMAGE_DIM = (3, 224, 224)

daft.set_runner_ray()

# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass
ray.get([warmup.remote() for _ in range(64)])

weights = ResNet18_Weights.DEFAULT
# 调整数据尺寸
transform = transforms.Compose([
    transforms.ToPILImage(),        # 新增：把 ndarray 转成 PIL Image
    transforms.Resize((224, 224)),   # 可选：固定尺寸
    transforms.ToTensor(),           # 变为 Tensor
    weights.transforms()            # 预训练权重 normalization
])

# 取消掉GPU设置
@daft.cls(
    max_concurrency=NUM_GPU_NODES,
    gpus=0,
)
class ResNetModel:
    def __init__(self):
        # 注释掉真实的模型初始化
        self.weights = weights
        self.device = torch.device("cpu")
        self.model = None

    @daft.method.batch(
        return_dtype=daft.DataType.string(),
        batch_size=BATCH_SIZE,
    )
    def __call__(self, images):
        batch_len = len(images)
        if batch_len == 0:
            return []
        mock_class_index = 0
        predicted_label = self.weights.meta["categories"][mock_class_index]
        # 返回与批次大小一致的列表
        return [predicted_label] * batch_len


# 本地 IO，取消掉s3配置，避免报错
daft.set_planning_config(default_io_config=daft.io.IOConfig())

start_time = time.time()

df = daft.read_parquet(INPUT_PATH)
df = df.with_column(
    "decoded_image",
    df["image_url"].download().decode_image(mode=daft.ImageMode.RGB),
)
df = df.with_column(
    "norm_image",
    df["decoded_image"].apply(
        func=lambda image: transform(image),
        return_dtype=daft.DataType.tensor(dtype=daft.DataType.float32(), shape=IMAGE_DIM),
    ),
)
df = df.with_column("label", ResNetModel()(col("norm_image")))
df = df.select("image_url", "label")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
end = time.perf_counter() - start
print(f"ALL PYTHON Time taken: {end} seconds")
