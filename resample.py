from pathlib import Path

from joblib import Parallel, delayed
import torchaudio
import torch
import argparse


def _trim_audio_by_info(
    audio_path: Path, sample_rate: int, input_dir: str, output_dir: str
) -> None:
    output_dir = audio_path.parent.as_posix().replace(input_dir, output_dir)
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    waveform, original_sample_rate = torchaudio.load(audio_path)  # type: ignore
    assert isinstance(original_sample_rate, int)

    if waveform.shape[0] > 1:
        waveform = torch.mean(waveform, dim=0, keepdim=True)

    if original_sample_rate != sample_rate:
        waveform = torchaudio.transforms.Resample(original_sample_rate, sample_rate)(
            waveform
        )

    output_path = Path(output_dir) / audio_path.name

    torchaudio.save(  # type: ignore
        output_path,
        waveform,
        sample_rate,
        encoding="PCM_S",
        bits_per_sample=16,
    )


def trim_audio_by_info(
    input_dir: str,
    output_dir: str,
    sample_rate: int = 16000,
    n_jobs: int = 1,
    verbose: bool = False,
) -> None:
    """
    Trim audio files based on the provided information.

    Args:
        info (dict[str, dict]): A dictionary containing information about the audio files to be trimmed.
        output_dir (Path): The directory where the trimmed audio files will be saved.
        sample_rate (int, optional): The desired sample rate of the trimmed audio files. Defaults to 16000.
        n_jobs (int, optional): The number of parallel jobs to use for trimming. Defaults to 1.
        verbose (bool, optional): Whether to display verbose output. Defaults to False.
    """
    if Path(output_dir).exists():
        return

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    Parallel(backend="threading", n_jobs=n_jobs, verbose=verbose)(
        delayed(_trim_audio_by_info)(path, sample_rate, input_dir, output_dir)
        for path in Path(input_dir).glob("**/*.wav")
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trim audio files")
    parser.add_argument("input_dir", type=str, help="Input directory")
    parser.add_argument("output_dir", type=str, help="Output directory")
    parser.add_argument("-s", "--sample_rate", type=int, default=22050, help="Desired sample rate")
    parser.add_argument("-n", "--n_jobs", type=int, default=64, help="Number of parallel jobs")
    parser.add_argument("-v", "--verbose", action="store_true", help="Display verbose output")
    
    args = parser.parse_args()
    
    trim_audio_by_info(args.input_dir, args.output_dir, args.sample_rate, args.n_jobs, args.verbose)
