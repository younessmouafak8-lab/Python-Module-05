from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self.type = self.__class__.__name__.removesuffix("Processor")
        self.message: str | None = None
        self.validation: str | None = None

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}\n"


class NumericProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        temp = []
        self.message = None
        if not self.validate(data):
            raise ValueError("Values must be either a list or a int or float")
        else:
            self.validation = "Numeric data verified"
        if isinstance(data, (int, float)):
            temp = [data]
            the_sum = sum(temp)
            self.message = (f"Processed 1 numeric value, sum={the_sum}, "
                            f"avg={the_sum / 1}")
            return f"Processing data: {temp}"
        the_sum = sum(data)
        the_len = len(data)
        if the_len != 0:
            average = the_sum / the_len
        else:
            average = 0
        self.message = (f"Processed {the_len} numeric values, sum={the_sum}, "
                        f"avg={average:.1f}")
        return f"Processing data: {data}"

    def validate(self, data: Any) -> bool:
        if self.message is not None:
            print(f"Validation: {self.validation}")
        if not isinstance(data, (list, int, float)) or isinstance(data, bool):
            return False
        if isinstance(data, list) and \
                not all([type(x) in (int, float) for x in data]):
            return False
        return True

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class TextProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        self.message = None
        if not self.validate(data):
            raise ValueError("Value must be a string")
        self.validation = "text data verified"
        chars = len(data)
        words = len(data.split())
        self.message = f"Processed text: {chars} characters, {words} words"
        return f'Processing data: "{data}"'

    def validate(self, data: Any) -> bool:
        if self.message is not None:
            print(f"Validation: {self.validation}")
        if isinstance(data, str):
            return True
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


class LogProcessor(DataProcessor):

    def process(self, data: Any) -> str:
        self.message = None
        if not self.validate(data):
            raise ValueError("Value must be a string")
        self.validation = "Log entry verified"
        data2 = data.split(":")
        if len(data2) != 2:
            raise ValueError("Incorrect format: 'level: message")
        if data2[0] == "" or data2[1] == "":
            raise ValueError("Empty Error Type or "
                             "message will not be tolerated")
        mssg = data2[1]
        if data.startswith("ERROR"):
            level = "ALERT"
            self.message = f"[{level}] ERROR level detected: {mssg}"
        else:
            level = data2[0]
            self.message = f"[{level}] {level} level detected: {mssg}"
        return f'Processing data: "{data}"'

    def validate(self, data: Any) -> bool:
        if self.message is not None:
            print(f"Validation: {self.validation}")
        if isinstance(data, str):
            return True
        return False

    def format_output(self, result: str) -> str:
        return super().format_output(result)


def process_manager(processor: DataProcessor, data: Any) -> None:
    try:
        print(f"Initializing {processor.type} Processor...")
        print(processor.process(data))
        print(f"Validation: {processor.validation}")
        print(processor.format_output(processor.message))
    except Exception as e:
        print(e.__class__.__name__, f": {e}\n")


def polymorphic_processing(processor: DataProcessor, data: Any) -> None:
    processor.process(data)


print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
processors = [NumericProcessor(), TextProcessor(), LogProcessor()]
data = [[1, 2, 3, 4, 5], "Hello Nexus World", "ERROR: Connection timeout"]

for processor, data in zip(processors, data):
    process_manager(processor, data)

print("=== Polymorphic Processing Demo ===  ")
data = [[1, 2, 3], "Hello World!", "INFO: System ready"]
i = 1
for processor, data in zip(processors, data):
    try:
        polymorphic_processing(processor, data)
        print(f"Result {i}:", processor.message)
    except Exception as e:
        print(f"{e.__class__.__name__}: {e}\n")
    finally:
        i += 1
print("\nFoundation systems online. Nexus ready for advanced streams.")
