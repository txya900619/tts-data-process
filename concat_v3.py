import argparse
import json
import os
import re
import shutil
from decimal import Decimal, getcontext

import jsonlines

MIN_DURATION = 2  # hakka 5
CONCAT_TRGGER_MAX_DURATION = 6  # hakka 20


def concat_metadata(metadata_list, wav_save_dir):
    source_audio_paths = [metadata["audio_path"] for metadata in metadata_list]
    start = min([metadata["start"] for metadata in metadata_list])
    end = max([metadata["end"] for metadata in metadata_list])
    duration = float(sum([Decimal(metadata["duration"]) for metadata in metadata_list]))
    text = " ".join([metadata["text"] for metadata in metadata_list])
    text = re.sub(r"\s+", " ", text)

    ipa = " <sil> ".join(
        [metadata["ipa"].replace("<sil>", "") for metadata in metadata_list]
    )
    if metadata_list[-1]["ipa"].endswith("<sil>"):
        ipa += " <sil>"
    # ipa = " ".join([metadata["ipa"] for metadata in metadata_list])
    ipa = re.sub(r"\s+", " ", ipa)

    ipa_list = []
    speaker = metadata_list[0]["speaker"]
    dialect = metadata_list[0]["dialect"]
    new_id = f"{metadata_list[0]['id']}_{start}-{end}"
    save_paths = os.path.join(wav_save_dir, f"{new_id}.wav")

    concated_duration = 0
    for metadata in metadata_list:
        if isinstance(metadata["ipa_list"], str):
            metadata["ipa_list"] = metadata["ipa_list"]
        ipa_list.extend(
            [
                [
                    str(Decimal(ipa_with_start_end[0]) + Decimal(concated_duration)),
                    str(Decimal(ipa_with_start_end[1]) + Decimal(concated_duration)),
                    ipa_with_start_end[2],
                ]
                for ipa_with_start_end in metadata["ipa_list"]
            ]
        )
        concated_duration += metadata["duration"]

    return {
        "id": new_id,
        "duration": duration,
        "text": text,
        "ipa": ipa,
        "ipa_list": ipa_list,
        "speaker": speaker,
        "audio_path": save_paths,
        "dialect": dialect,
    }, source_audio_paths


def get_concat_metadata(
    id_metadata_mapping: dict[str, dict[str, list]], wav_save_dir: str
):
    concated_metadata_list = []
    id_source_audio_paths_mapping = {}
    tmp_metadata_to_concat = []

    for id, metadata in id_metadata_mapping.items():
        for i in range(len(metadata["duration"])):
            if metadata["duration"][i] > CONCAT_TRGGER_MAX_DURATION:
                if len(tmp_metadata_to_concat) > 1:
                    concated_metadata, source_audio_paths = concat_metadata(
                        tmp_metadata_to_concat, wav_save_dir
                    )
                    concated_metadata_list.append(concated_metadata)
                    id_source_audio_paths_mapping[concated_metadata["id"]] = (
                        source_audio_paths
                    )
                    tmp_metadata_to_concat = []

                new_id = f"{id}_{metadata['start'][i]}-{metadata['end'][i]}"
                concated_metadata_list.append(
                    {
                        "id": new_id,
                        "duration": metadata["duration"][i],
                        "text": metadata["text"][i],
                        "ipa": metadata["ipa"][i],
                        "ipa_list": metadata["ipa_list"][i],
                        "speaker": metadata["speaker"],
                        "audio_path": os.path.join(wav_save_dir, f"{new_id}.wav"),
                        "dialect": metadata["dialect"],
                    }
                )
                id_source_audio_paths_mapping[new_id] = [metadata["audio_path"][i]]
                continue

            if not metadata["ipa"][i].endswith("<sil>"):
                if len(tmp_metadata_to_concat) > 1:
                    tmp_metadata_to_concat.append(
                        {
                            "id": id,
                            "duration": metadata["duration"][i],
                            "text": metadata["text"][i],
                            "ipa": metadata["ipa"][i],
                            "ipa_list": metadata["ipa_list"][i],
                            "speaker": metadata["speaker"],
                            "audio_path": metadata["audio_path"][i],
                            "start": metadata["start"][i],
                            "end": metadata["end"][i],
                            "dialect": metadata["dialect"],
                        }
                    )
                    concated_metadata, source_audio_paths = concat_metadata(
                        tmp_metadata_to_concat, wav_save_dir
                    )
                    concated_metadata_list.append(concated_metadata)
                    id_source_audio_paths_mapping[concated_metadata["id"]] = (
                        source_audio_paths
                    )
                    tmp_metadata_to_concat = []
                else:
                    new_id = f"{id}_{metadata['start'][i]}-{metadata['end'][i]}"
                    concated_metadata_list.append(
                        {
                            "id": new_id,
                            "duration": metadata["duration"][i],
                            "text": metadata["text"][i],
                            "ipa": metadata["ipa"][i],
                            "ipa_list": metadata["ipa_list"][i],
                            "speaker": metadata["speaker"],
                            "audio_path": os.path.join(wav_save_dir, f"{new_id}.wav"),
                            "dialect": metadata["dialect"],
                        }
                    )
                    id_source_audio_paths_mapping[new_id] = [metadata["audio_path"][i]]
                    continue

            tmp_metadata_to_concat.append(
                {
                    "id": id,
                    "duration": metadata["duration"][i],
                    "text": metadata["text"][i],
                    "ipa": metadata["ipa"][i],
                    "ipa_list": metadata["ipa_list"][i],
                    "speaker": metadata["speaker"],
                    "audio_path": metadata["audio_path"][i],
                    "start": metadata["start"][i],
                    "end": metadata["end"][i],
                    "dialect": metadata["dialect"],
                }
            )
            if (
                sum([metadata["duration"] for metadata in tmp_metadata_to_concat])
                >= MIN_DURATION
            ):
                concated_metadata, source_audio_paths = concat_metadata(
                    tmp_metadata_to_concat, wav_save_dir
                )
                concated_metadata_list.append(concated_metadata)
                id_source_audio_paths_mapping[concated_metadata["id"]] = (
                    source_audio_paths
                )
                tmp_metadata_to_concat = []

        if len(tmp_metadata_to_concat) >= 1:
            concated_metadata, source_audio_paths = concat_metadata(
                tmp_metadata_to_concat, wav_save_dir
            )
            concated_metadata_list.append(concated_metadata)
            id_source_audio_paths_mapping[concated_metadata["id"]] = source_audio_paths
            tmp_metadata_to_concat = []

    return concated_metadata_list, id_source_audio_paths_mapping


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Audio Concatenation")
    parser.add_argument(
        "-c", "--config", type=str, nargs="+", help="List of config files"
    )
    parser.add_argument(
        "-o", "--output_folder", type=str, help="Output path for concatenated wav files"
    )
    args = parser.parse_args()

    getcontext().prec = 3

    for config in args.config:
        id_metadata_mapping = {}
        with jsonlines.open(f"{config}.json") as reader:
            for metadata in reader:
                id, start_end = metadata["id"].split("_")
                start, end = start_end.split("-")
                duration = metadata["duration"]
                text = metadata["text"]
                ipa = metadata["ipa"]
                dialect = metadata["dialect"]
                ipa_list = json.loads(metadata["ipa_list"])
                ipa_list = [
                    [
                        str(ipa_with_start_end[0]),
                        str(ipa_with_start_end[1]),
                        ipa_with_start_end[2],
                    ]
                    for ipa_with_start_end in ipa_list
                ]

                if "speaker" not in metadata:
                    metadata["speaker"] = f"hac_vocab_{config}"
                speaker = metadata["speaker"]
                audio_path = metadata["audio_path"]
                if id not in id_metadata_mapping:
                    id_metadata_mapping[id] = {
                        "duration": [duration],
                        "text": [text],
                        "ipa": [ipa],
                        "speaker": speaker,
                        "audio_path": [audio_path],
                        "start": [float(start)],
                        "end": [float(end)],
                        "ipa_list": [ipa_list],
                        "dialect": dialect,
                    }
                else:
                    id_metadata_mapping[id]["duration"].append(duration)
                    id_metadata_mapping[id]["text"].append(text)
                    id_metadata_mapping[id]["ipa"].append(ipa)
                    id_metadata_mapping[id]["audio_path"].append(audio_path)
                    id_metadata_mapping[id]["start"].append(float(start))
                    id_metadata_mapping[id]["end"].append(float(end))
                    id_metadata_mapping[id]["ipa_list"].append(ipa_list)

        concat_metadata_list = []
        wav_save_dir = os.path.join(args.output_folder, config)
        if not os.path.exists(wav_save_dir):
            os.makedirs(wav_save_dir)

        concat_metadata_list, id_source_audio_paths_mapping = get_concat_metadata(
            id_metadata_mapping, wav_save_dir
        )

        with jsonlines.open(f"{config}_concat.json", "w") as writer:
            writer.write_all(concat_metadata_list)
            writer.close()

        concat_info = ""
        for metadata in concat_metadata_list:
            if len(id_source_audio_paths_mapping[metadata["id"]]) == 1:
                shutil.copy2(
                    id_source_audio_paths_mapping[metadata["id"]][0],
                    metadata["audio_path"],
                )
                continue

            source_audio_paths = " ".join(id_source_audio_paths_mapping[metadata["id"]])

            concat_and_output_path = f"{source_audio_paths} {metadata['audio_path']}"
            concat_info += concat_and_output_path + "\n"
        with open(f"{config}_concat_info.tsv", "w") as f:
            f.write(concat_info)
            f.close()
