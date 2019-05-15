import decimal
import math

from wavefile import WaveReader

def wave_duration(filepath: str, human: bool = False) -> str:
  wave_file: WaveReader = WaveReader(filepath)
  duration: float = wave_file.frames / wave_file.samplerate
  if human == False:
    return duration

  duration: float = duration / 60
  frac, whole = math.modf(duration)
  minutes: int = int(whole)
  seconds: int = int(frac * 60) + 1
  return f'{minutes}:{seconds}'

