"""A fake robot with the same method names as NVIDIA jetbot's Robot class.

Used by: robot/client.py (RobotClient builds one and maps command names to its methods).
Real jetbot: https://github.com/NVIDIA-AI-IOT/jetbot (jetbot/robot.py). Motor values range -1.0 to 1.0.
To drive real hardware, swap this for `from jetbot import Robot`; the method names match.
"""
import logging

log = logging.getLogger("Jetbot")


class FakeJetbot:
    def __init__(self) -> None:
        self.left_motor = 0.0
        self.right_motor = 0.0

    def set_motors(self, left: float, right: float) -> None:
        if (left, right) != (self.left_motor, self.right_motor):
            log.info("motors left=%.2f right=%.2f", left, right)
        self.left_motor, self.right_motor = left, right

    def forward(self, speed: float = 0.5) -> None:
        self.set_motors(speed, speed)

    def backward(self, speed: float = 0.5) -> None:
        self.set_motors(-speed, -speed)

    def left(self, speed: float = 0.5) -> None:
        self.set_motors(-speed, speed)

    def right(self, speed: float = 0.5) -> None:
        self.set_motors(speed, -speed)

    def stop(self) -> None:
        self.set_motors(0.0, 0.0)

    def __repr__(self) -> str:
        return f"FakeJetbot(left={self.left_motor}, right={self.right_motor})"
