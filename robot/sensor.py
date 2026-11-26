"""A simulated sensor: a sine wave.

Used by: robot/client.py (RobotClient publishes one reading every SENSOR_INTERVAL).

ASSUMPTION A8: the assignment's demo publishes a fixed {"state": 25.5}. A sine wave centered on
25.5 still matches that example, and it crosses the Player's "high" threshold, so the Player's
processing and alerts visibly do something. Give each robot its own amplitude/period
(robot --amplitude/--period) to see the difference, e.g. robot-1 at 3 never goes "high".
"""
import math
import time

from common.config import SENSOR_AMPLITUDE, SENSOR_CENTER, SENSOR_PERIOD


class SineSensor:
    def __init__(self, center: float = SENSOR_CENTER, amplitude: float = SENSOR_AMPLITUDE,
                 period: float = SENSOR_PERIOD, clock=time.monotonic):
        if period <= 0:
            raise ValueError("period must be positive")
        self.center, self.amplitude, self.period = center, amplitude, period
        self.clock = clock          # swappable so tests can control time
        self.start = clock()

    def read(self) -> float:
        t = self.clock() - self.start
        return round(self.center + self.amplitude * math.sin(2 * math.pi * t / self.period), 2)
