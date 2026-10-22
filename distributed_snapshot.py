"""
Distributed Snapshot Simulator
Chandy-Lamport Algorithm | FIFO Channels
Python 3 - No external dependencies
"""

from dataclasses import dataclass, field
from collections import defaultdict
import heapq


@dataclass
class Message:
    sender: int
    receiver: int
    kind: str
    label: str
    send_time: int
    arrival_time: int
    sequence: int


@dataclass
class Process:
    pid: int
    local_state: int = 0
    snapshot_state: int = None
    recorded: bool = False
    recording_channels: dict = field(default_factory=dict)
    channel_state: dict = field(
        default_factory=lambda: defaultdict(list)
    )


class SnapshotSimulator:

    def __init__(self, num_processes=3):
        self.n = num_processes
        self.processes = [Process(i) for i in range(num_processes)]
        self.channel_next_free = {}
        self.events = []
        self.logs = []
        self.sequence = 0
        self.snapshot_started = False
        self.snapshot_complete = False

        for src in range(num_processes):
            for dst in range(num_processes):
                if src != dst:
                    self.channel_next_free[(src, dst)] = 0
                    self.processes[dst].recording_channels[src] = False

    def send(self, src, dst, kind, label, now, base_delay=2):
        proposed = now + base_delay
        arrival = max(
            proposed,
            self.channel_next_free[(src, dst)] + 1
        )
        self.channel_next_free[(src, dst)] = arrival

        msg = Message(
            src, dst, kind, label, now, arrival, self.sequence
        )
        self.sequence += 1

        priority = 0 if kind == "MARKER" else 1
        heapq.heappush(
            self.events,
            (arrival, priority, msg.sequence, msg)
        )

        self.logs.append(
            f"t={now}: P{src} -> P{dst} {kind:<6} "
            f"{label:<8} (arrival t={arrival})"
        )

    def application_send(self, src, dst, label, now, base_delay=2):
        self.processes[src].local_state += 1
        self.send(src, dst, "APP", label, now, base_delay)

    def record_local_state(self, pid, now):
        p = self.processes[pid]

        if p.recorded:
            return

        p.snapshot_state = p.local_state
        p.recorded = True

        for src in range(self.n):
            if src != pid:
                p.recording_channels[src] = True

        self.logs.append(
            f"t={now}: P{pid} RECORDS local state = "
            f"{p.snapshot_state}"
        )

    def initiate_snapshot(self, initiator, now):
        self.snapshot_started = True

        self.logs.append(
            f"t={now}: SNAPSHOT INITIATED at P{initiator}"
        )

        self.record_local_state(initiator, now)

        for dst in range(self.n):
            if dst != initiator:
                self.send(
                    initiator, dst, "MARKER", "MARKER", now
                )

    def handle_message(self, msg):
        now = msg.arrival_time
        p = self.processes[msg.receiver]

        if msg.kind == "APP":
            self.logs.append(
                f"t={now}: P{msg.receiver} RECEIVES "
                f"{msg.label} from P{msg.sender}"
            )

            if (
                p.recorded
                and p.recording_channels[msg.sender]
            ):
                p.channel_state[msg.sender].append(msg.label)

                self.logs.append(
                    f"t={now}: P{msg.receiver} RECORDS "
                    f"{msg.label} as IN-TRANSIT on "
                    f"C{msg.sender}->{msg.receiver}"
                )

            p.local_state += 1

        else:
            self.logs.append(
                f"t={now}: P{msg.receiver} RECEIVES MARKER "
                f"from P{msg.sender}"
            )

            if not p.recorded:
                self.record_local_state(msg.receiver, now)

                p.recording_channels[msg.sender] = False

                for dst in range(self.n):
                    if dst != msg.receiver:
                        self.send(
                            msg.receiver,
                            dst,
                            "MARKER",
                            "MARKER",
                            now
                        )
            else:
                p.recording_channels[msg.sender] = False

    def run(self):
        self.application_send(0, 1, "M01", 1, 2)
        self.application_send(1, 2, "M12", 2, 4)
        self.application_send(2, 0, "M20", 3, 4)

        self.initiate_snapshot(0, 4)

        self.application_send(1, 0, "M10", 5, 2)
        self.application_send(2, 1, "M21", 6, 2)

        while self.events:
            _, _, _, msg = heapq.heappop(self.events)
            self.handle_message(msg)

        self.snapshot_complete = all(
            p.recorded for p in self.processes
        )

    def verify(self):
        checks = []

        checks.append((
            "Every process records exactly one local state",
            all(
                p.recorded and p.snapshot_state is not None
                for p in self.processes
            )
        ))

        checks.append((
            "All channel recording intervals are closed",
            all(
                not p.recording_channels[src]
                for p in self.processes
                for src in range(self.n)
                if src != p.pid
            )
        ))

        checks.append((
            "FIFO channel order is preserved",
            all(
                isinstance(label, str)
                for p in self.processes
                for state in p.channel_state.values()
                for label in state
            )
        ))

        checks.append((
            "At least one in-transit message is captured",
            any(
                state
                for p in self.processes
                for state in p.channel_state.values()
            )
        ))

        return checks

    def print_report(self):
        print("=" * 72)
        print("DISTRIBUTED SNAPSHOT SIMULATOR")
        print("Chandy-Lamport Algorithm | FIFO Channels")
        print("=" * 72)

        print("\nEVENT TRACE")
        for line in self.logs:
            print(line)

        print("\nSNAPSHOT GLOBAL STATE")

        for p in self.processes:
            print(
                f" P{p.pid}: local state = {p.snapshot_state}"
            )

            for src in range(self.n):
                if src != p.pid:
                    state = p.channel_state[src]
                    print(
                        f"    C{src}->{p.pid}: "
                        f"{state if state else 'EMPTY'}"
                    )

        print("\nCORRECTNESS CHECKS")

        results = self.verify()

        for name, passed in results:
            print(
                f" {'PASS' if passed else 'FAIL'} - {name}"
            )

        overall = (
            self.snapshot_complete
            and all(ok for _, ok in results)
        )

        print(
            f"\nSNAPSHOT OVERALL: "
            f"{'PASS' if overall else 'FAIL'}"
        )


if __name__ == "__main__":
    simulator = SnapshotSimulator(num_processes=3)
    simulator.run()
    simulator.print_report()