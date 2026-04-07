from config import (
    MODEL_NAME,
    SAMPLING_PARAMS,
    INPUT_PATH,
    CONCURRENCY,
    print_benchmark_results,
)
from vllm import LLM
import daft
from daft import Series
import time
import hashlib

# 取消掉GPU设置
@daft.cls(max_concurrency=CONCURRENCY, gpus=0)
class VLLM:
    def __init__(self):
        # 不做任何昂贵初始化
        print("Initializing Mock LLM (CPU-only, no real model)...")

    @daft.method.batch(return_dtype=str, batch_size=512)
    def generate(self, prompts: Series) -> Series:
        """
        模拟一次 batch 推理：
        - 对每个 prompt 做一个轻量计算
        - 可选 sleep 来模拟推理延迟
        """
        prompt_list = prompts.to_pylist()

        outputs = []
        for p in prompt_list:
            # 模拟一些 CPU work（而不是空 return）
            h = hashlib.sha256(p.encode("utf-8")).hexdigest()
            outputs.append(f"mock_output_{h[:16]}")

        # # 模拟 batch 推理耗时（你可以调这个值）
        # time.sleep(0.01)

        return Series.from_pylist(outputs)


def main():
    print(f"Starting benchmark...")

    daft.set_runner_ray()

    df = daft.read_parquet(INPUT_PATH).into_partitions(32)

    vllm = VLLM()
    df = df.sort("prompt")
    df = df.with_column("output", vllm.generate(df["prompt"]))

    print("Running benchmark...")
    start_time = time.perf_counter()
    df = df.collect()
    end_time = time.perf_counter()
    print("Benchmark completed!")

    df = df.with_columns(
        {
            "prompt_len": df["prompt"].length(),
            "output_len": df["output"].length(),
        }
    )
    df.show()

    print_benchmark_results("naive-batch-sorted.py", start_time, end_time)


if __name__ == "__main__":
    start_time = time.perf_counter()
    main()
    end_time = time.perf_counter() - start_time
    print(f"ALL PYTHON Time taken: {end_time} seconds")