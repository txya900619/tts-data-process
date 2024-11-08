import argparse
import os

import datasets
import soundfile as sf

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process TTS data from Hugging Face")
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
    parser.add_argument(
        "-s", "--split", default="train", help="Name of the split to load"
    )
    parser.add_argument(
        "-c",
        "--configs",
        default=[],
        nargs="*",
        help="List of dataset configurations to load",
    )
    parser.add_argument(
        "-f",
        "--filter-keys",
        default=[],
        nargs="*",
    )
    parser.add_argument(
        "-g",
        "--grater-than",
        default=[],
        nargs="*",
    )

    args = parser.parse_args()
    configs = (
        datasets.get_dataset_config_names(args.dataset)
        if len(args.configs) == 0
        else args.configs
    )
    for config in configs:
        d = datasets.load_dataset(args.dataset, config, split=args.split)
        if not os.path.exists(f"{args.output_dir}/{config}"):
            os.makedirs(f"{args.output_dir}/{config}")
        if len(args.filter_keys) > 0 and len(args.grater_than) == len(args.filter_keys):
            d = d.filter(
                lambda x: all(
                    x[k] > float(v) if k in x else False
                    for k, v in zip(args.filter_keys, args.grater_than)
                )
            )

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
