import argparse
import os

import jsonlines
import torch
import torchaudio
from joblib import Parallel, delayed


def concat_audio(audio_paths, save_path):
    audios = None
    for audio_path in audio_paths:
        waveform, original_sample_rate = torchaudio.load(audio_path)
        if audios is None:
            audios = waveform
        else:
            audios = torch.cat((audios, waveform), dim=1)
    torchaudio.save(  # type: ignore
        save_path,
        audios,
        original_sample_rate,
        encoding="PCM_S",
        bits_per_sample=16,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audio Concatenation")
    parser.add_argument("--config", type=str, nargs="+", help="List of config names")
    args = parser.parse_args()

    for config in args.config:
        id_metadata_mapping = {}
        with jsonlines.open(f"{config}.json") as reader:
            for metadata in reader:
                id = metadata["id"].split("_")[0]
                duration = metadata["duration"]
                text = metadata["text"]
                ipa = metadata["ipa"]
                speaker = metadata["speaker"]
                audio_path = metadata["audio_path"]
                if id not in id_metadata_mapping:
                    id_metadata_mapping[id] = {
                        "duration": duration,
                        "text": [text],
                        "ipa": [ipa],
                        "speaker": speaker,
                        "audio_path": [audio_path],
                    }
                else:
                    id_metadata_mapping[id]["duration"] += duration
                    id_metadata_mapping[id]["text"].append(text)
                    id_metadata_mapping[id]["ipa"].append(ipa)
                    id_metadata_mapping[id]["audio_path"].append(audio_path)

        concat_metadata = []
        wav_save_dir = f"wav_concat/{config}"
        if not os.path.exists(wav_save_dir):
            os.makedirs(wav_save_dir)

        for id, metadata in id_metadata_mapping.items():
            save_path = os.path.join(wav_save_dir, f"{id}.wav")
            concat_metadata.append(
                {
                    "id": id,
                    "duration": metadata["duration"],
                    "text": "，".join(metadata["text"]),
                    "ipa": "，".join(metadata["ipa"]),
                    "speaker": metadata["speaker"],
                    "audio_path": save_path,
                    "any_oovs": False,
                }
            )

        Parallel(backend="threading", n_jobs=64, verbose=True)(
            delayed(concat_audio)(
                id_metadata_mapping[metadata["id"]]["audio_path"],
                metadata["audio_path"],
            )
            for metadata in concat_metadata
        )

        with jsonlines.open(f"{config}_concat.json", "w") as writer:
            writer.write_all(concat_metadata)
            writer.close()
