
import os
import re
import subprocess
import sys
import tempfile
import time

PYTHON = sys.executable

ENV = os.environ.copy()
ENV["RABBITMQ_USER"] = "guest"
ENV["RABBITMQ_PASSWORD"] = "guest"


def run(module, mode, *extra):
    result = subprocess.run(
        [PYTHON, "-m", module, mode, *extra],
        env=ENV,
        capture_output=True,
        text=True,
    )
    print(result.stdout, end="", flush=True)

    if result.returncode != 0:
        print(result.stderr, end="", flush=True)
        raise RuntimeError(f"{mode} failed")

    return result.stdout


def get_id(output):
    match = re.search(r"Message ID:\s*(\S+)", output)

    if not match:
        raise RuntimeError("Message ID not found")

    return match.group(1)


def test_crash():
    print("\n=== TEST 1: CONSUMER CRASH ===", flush=True)

    module = "resilience.test_consumer_crash"

    consumer = subprocess.Popen(
        [PYTHON, "-u", "-m", module, "crash"],
        env=ENV,
    )

    try:
        time.sleep(2)

        sent = run(module, "send")
        message_id = get_id(sent)

        consumer.wait(timeout=10)

        if consumer.returncode != 1:
            raise RuntimeError("Expected consumer crash did not occur")

        checked = run(module, "check")

        if get_id(checked) != message_id:
            raise RuntimeError("Wrong message was redelivered")

        if "Redelivered: True" not in checked:
            raise RuntimeError("Redelivery not confirmed")

        print("\nTEST 1 PASSED", flush=True)

    finally:
        if consumer.poll() is None:
            consumer.terminate()
            consumer.wait()


def test_invalid():
    print("\n=== TEST 2: INVALID MESSAGE ===", flush=True)

    module = "resilience.test_invalid_message"

    with tempfile.TemporaryFile(mode="w+t") as logs:
        consumer = subprocess.Popen(
            [
                PYTHON, "-u", "-m",
                "analysis.consumer", "temperature"
            ],
            env=ENV,
            stdout=logs,
            stderr=subprocess.STDOUT,
        )

        try:
            time.sleep(2)

            sent = run(module, "send")
            message_id = get_id(sent)

            checked = None

            for _ in range(10):
                time.sleep(1)

                result = subprocess.run(
                    [
                        PYTHON, "-m", module,
                        "check", "--message-id", message_id
                    ],
                    env=ENV,
                    capture_output=True,
                    text=True,
                )

                if result.returncode == 0:
                    checked = result.stdout
                    break

            if checked is None:
                raise RuntimeError("Message not found in DLQ")

            print(checked, end="", flush=True)

            if "Test passed:" not in checked:
                raise RuntimeError("DLQ verification failed")

            run(module, "recover")

            processed = False

            for _ in range(10):
                time.sleep(1)
                logs.seek(0)
                output = logs.read()

                if "Processed sensor reading" in output:
                    processed = True
                    print("Valid message processed", flush=True)
                    break

            if not processed:
                raise RuntimeError("Recovery was not confirmed")

            print("\nTEST 2 PASSED", flush=True)

        finally:
            if consumer.poll() is None:
                consumer.terminate()
                consumer.wait()

            logs.seek(0)
            for line in logs:
                if "ERROR analysis:" in line:
                    print(line.strip(), flush=True)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in (
        "crash", "invalid"
    ):
        sys.exit("Usage: python -m resilience.run_tests crash|invalid")

    try:
        if sys.argv[1] == "crash":
            test_crash()
        else:
            test_invalid()

    except Exception as error:
        print(f"\nTEST FAILED: {error}", flush=True)
        sys.exit(1)
