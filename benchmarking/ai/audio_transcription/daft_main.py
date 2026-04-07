from __future__ import annotations

import io

import numpy as np
import torch
import torchaudio
import torchaudio.transforms as T
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor
import time
import ray

import daft

start = time.perf_counter()
# 需要自行下载模型，并修改路径为自己的模型路径，避免网络影响
TRANSCRIPTION_MODEL = "/home/data/whisper-tiny"
# 由于下方模型UDF处gpus设置为0，所以此时该参数用于设置一次起多少个基于CPU的解析任务，由于mock了模型可以设置的大些
NUM_GPUS = 32
NEW_SAMPLING_RATE = 16000
# 数据输入路径，需要修改成本地路径
INPUT_PATH = "/home/data/audio_benchmark/parquet"
# 输出路径，需要修改成本地路径
OUTPUT_PATH = "/home/data/audio_benchmark/audio_benchmark_results"

daft.set_runner_ray()

# Wait for Ray cluster to be ready
@ray.remote
def warmup():
    pass
ray.get([warmup.remote() for _ in range(64)])


def resample(audio_bytes):
    waveform, sampling_rate = torchaudio.load(io.BytesIO(audio_bytes), format="flac")
    waveform = T.Resample(sampling_rate, NEW_SAMPLING_RATE)(waveform).squeeze()
    return np.array(waveform)


processor = AutoProcessor.from_pretrained(TRANSCRIPTION_MODEL)


@daft.func.batch(return_dtype=daft.DataType.tensor(daft.DataType.float32()))
def whisper_preprocess(resampled):
    extracted_features = processor(
        resampled.to_arrow().to_numpy(zero_copy_only=False).tolist(),
        sampling_rate=NEW_SAMPLING_RATE,
        device="cpu",
    ).input_features
    return extracted_features

# 取消掉GPU设置
@daft.cls(max_concurrency=NUM_GPUS, gpus=0)
class Transcriber:
    def __init__(self) -> None:
        # 不加载模型
        self.device = "cpu"
        self.dtype = torch.float16

    @daft.method.batch(
        return_dtype=daft.DataType.list(daft.DataType.int32()),
        batch_size=64,
    )
    def __call__(self, extracted_features):
        # 打桩返回固定 shape 的伪 token_ids
        batch_size = len(extracted_features)
        # 模拟长度 10 的 token id 序列
        fake_ids = np.full((batch_size, 10), fill_value=1, dtype=np.int32)
        return fake_ids


@daft.func.batch(return_dtype=daft.DataType.string())
def decoder(token_ids):
    transcription = processor.batch_decode(token_ids, skip_special_tokens=True)
    return transcription

# 本地 IO，取消掉s3配置，避免报错
daft.set_planning_config(default_io_config=daft.io.IOConfig())

start_time = time.time()

df = daft.read_parquet(INPUT_PATH)
df = df.with_column(
    "resampled",
    df["audio"]["bytes"].apply(resample, return_dtype=daft.DataType.list(daft.DataType.float32())),
)
df = df.with_column("extracted_features", whisper_preprocess(df["resampled"]))
df = df.with_column("token_ids", Transcriber()(df["extracted_features"]))
df = df.with_column("transcription", decoder(df["token_ids"]))
df = df.with_column("transcription_length", df["transcription"].length())
df = df.exclude("token_ids", "extracted_features", "resampled")
df.write_parquet(OUTPUT_PATH)

end_time = time.time()
print(f"Time taken: {end_time - start_time} seconds")
end = time.perf_counter() - start
print(f"ALL PYTHON Time taken: {end} seconds")
