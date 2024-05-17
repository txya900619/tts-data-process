import argparse
import os

import datasets
import soundfile as sf

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process TTS data from Hugging Face")
    parser.add_argument(
        "-c",
        "--configs",
        nargs="+",
        default=["sixian", "hailu", "fa_sixian", "fa_hailu"],
        help="List of configs to process",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        default="wav",
        help="Output directory for the generated WAV files",
    )
    parser.add_argument(
        "-d",
        "--dataset",
        default="formospeech/hat_tts",
        help="Name of the dataset to load",
    )
    args = parser.parse_args()

    for config in args.configs:
        d = datasets.load_dataset(args.dataset, config, split="train")
        if not os.path.exists(f"{args.output_dir}/{config}"):
            os.makedirs(f"{args.output_dir}/{config}")
        for x in d:
            sf.write(
                f"{args.output_dir}/{config}/{x['audio']['path']}",
                x["audio"]["array"],
                samplerate=x["audio"]["sampling_rate"],
            )
        d = d.map(
            lambda x: {
                "audio_path": f"{args.output_dir}/{config}/{x['audio']['path']}"
            },
            num_proc=16,
        )
        d = d.remove_columns("audio")
        d.to_json(f"{config}.json", force_ascii=False, lines=True)
