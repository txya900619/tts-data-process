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


def get_concat_metadata(id_metadata_mapping, max_concat_number):
    concat_metadata = []
    id_source_audio_paths_mapping = {}
    for concat_number in reversed(range(max_concat_number + 1)):
        remained_id_metadata_mapping = {}
        for id, metadata in id_metadata_mapping.items():
            if len(metadata["start"]) <= concat_number:
                remained_id_metadata_mapping[id] = metadata
                continue

            metadata["start"] = metadata["start"] + metadata["start"][:concat_number]
            metadata["end"] = metadata["end"] + metadata["end"][:concat_number]
            metadata["duration"] = (
                metadata["duration"] + metadata["duration"][:concat_number]
            )
            metadata["text"] = metadata["text"] + metadata["text"][:concat_number]
            metadata["ipa"] = metadata["ipa"] + metadata["ipa"][:concat_number]
            metadata["audio_path"] = (
                metadata["audio_path"] + metadata["audio_path"][:concat_number]
            )
            for i in range(len(metadata["start"]) - concat_number):
                start = metadata["start"][i]
                end = metadata["end"][i + concat_number]
                new_id = f"{id}_{start}-{end}"
                duration = sum(metadata["duration"][i : i + concat_number + 1])
                text = "，".join(metadata["text"][i : i + concat_number + 1])
                ipa = "，".join(metadata["ipa"][i : i + concat_number + 1])
                save_path = os.path.join(wav_save_dir, f"{new_id}.wav")
                id_source_audio_paths_mapping[new_id] = metadata["audio_path"][
                    i : i + concat_number + 1
                ]
                concat_metadata.append(
                    {
                        "id": new_id,
                        "duration": duration,
                        "text": text,
                        "ipa": ipa,
                        "speaker": metadata["speaker"],
                        "audio_path": save_path,
                        "any_oovs": False,
                    }
                )
        id_metadata_mapping = remained_id_metadata_mapping

    return concat_metadata, id_source_audio_paths_mapping


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audio Concatenation")
    parser.add_argument(
        "-c", "--config", type=str, nargs="+", help="List of config files"
    )
    parser.add_argument(
        "-m",
        "--max_concat_number",
        type=int,
        default=1,
        help="Maximum number of concatenations",
    )
    args = parser.parse_args()

    for config in args.config:
        id_metadata_mapping = {}
        with jsonlines.open(f"{config}.json") as reader:
            for metadata in reader:
                id, start_end = metadata["id"].split("_")
                start, end = start_end.split("-")
                duration = metadata["duration"]
                text = metadata["text"]
                ipa = metadata["ipa"]
                speaker = metadata["speaker"]
                audio_path = metadata["audio_path"]
                if id not in id_metadata_mapping:
                    id_metadata_mapping[id] = {
                        "duration": [duration],
                        "text": [text],
                        "ipa": [ipa],
                        "speaker": speaker,
                        "audio_path": [audio_path],
                        "start": [start],
                        "end": [end],
                    }
                else:
                    id_metadata_mapping[id]["duration"].append(duration)
                    id_metadata_mapping[id]["text"].append(text)
                    id_metadata_mapping[id]["ipa"].append(ipa)
                    id_metadata_mapping[id]["audio_path"].append(audio_path)
                    id_metadata_mapping[id]["start"].append(start)
                    id_metadata_mapping[id]["end"].append(end)

        concat_metadata = []
        wav_save_dir = f"wav_concat_v2/{config}"
        if not os.path.exists(wav_save_dir):
            os.makedirs(wav_save_dir)

        concat_metadata, id_source_audio_paths_mapping = get_concat_metadata(
            id_metadata_mapping, args.max_concat_number
        )

        with jsonlines.open(f"{config}_concat.json", "w") as writer:
            writer.write_all(concat_metadata)
            writer.close()

        Parallel(backend="threading", n_jobs=64, verbose=True)(
            delayed(concat_audio)(
                id_source_audio_paths_mapping[metadata["id"]],
                metadata["audio_path"],
            )
            for metadata in concat_metadata
        )
