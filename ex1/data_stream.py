from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self):
        self.type = self.__class__.__name__.removesuffix("Stream")
        self.alerts = []

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        ...

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        self.filtered_data = None
        return []

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"type": self.type}


class SensorStream(DataStream):
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.type = "Environmental Data"
        self.alerts = []

    def process_batch(self, data_batch: List[Any]) -> str:
        temps = []
        for i, item in enumerate(data_batch):
            if not isinstance(item, dict):
                raise TypeError(f"Item at index {i} must be a dictionary")
            j = 0
            for key, value in item.items():
                if j == 1:
                    raise TypeError(f"Dict at index {i} cant have more than "
                                    "one key:value pair")
                if not isinstance(value, (int, float)) or\
                        isinstance(value, bool):
                    raise ValueError(f"'{key}' at index {i} must be int or "
                                     f"float, got {type(value).__name__}")
                if key == "":
                    raise ValueError(f"key at index {i} cannot be empty")
                if key == "temp":
                    temps += [value]
                    if value > 70 or value < 0:
                        self.alerts += [value]
                j += 1
        if temps == []:
            temp = "not provided"
            unit = ""
        else:
            temp = sum(temps) / len(temps)
            unit = "°C"
        sensor_batch = [f"{key}:{value}" for dic in data_batch
                        for key, value in dic.items()]
        total = len(data_batch)
        self.processed_data = f"Processing sensor batch: {sensor_batch}"
        self.message = f"- Sensor data: {total} readings processed"
        return (f"Sensor analysis: {total} readings processed,"
                f" avg temp: {temp}{unit}\n")

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        self.process_batch(data_batch)
        if criteria == "critical":
            data = [f"{key}:{value}" for dic in data_batch
                    for key, value in dic.items()
                    if value > 70 or value < 0]
            total = len(data)
            if total != 0:
                self.filtered_data = f"{total} {criteria} sensor alerts"
            else:
                self.filtered_data = None
            return data
        return []

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "type": self.type,
            "id": self.stream_id,
            "processed_data": self.processed_data,
            "result": self.message
                }


class TransactionStream(DataStream):
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.type = "Financial Data"
        self.alerts = []

    def process_batch(self, data_batch: List[Any]) -> str:
        total_sales = 0
        total_bought = 0
        for i, item in enumerate(data_batch):
            if not isinstance(item, dict):
                raise TypeError(f"Item at index {i} must be a dictionary")
            j = 0
            for key, value in item.items():
                if j == 1:
                    raise ValueError(f"Dict at index {i} cant have "
                                    "more than one key:value pair")
                if not isinstance(value, (int, float)) or \
                        isinstance(value, bool):
                    raise TypeError(f"'{key}' at index {i} must be int or "
                                    f"float, got {type(value).__name__}")
                if key == "":
                    raise TypeError(f"key at index {i} cannot be empty")
                if key == "sell":
                    total_sales += value
                if key == "buy":
                    total_bought += value
                j += 1

        transaction_batch = [f"{key}:{value}" for dic in data_batch
                             for key, value in dic.items()]
        total = len(data_batch)
        net_flows = total_bought - total_sales
        self.processed_data = (f"Processing transaction "
                               f"batch: {transaction_batch}")
        self.message = f"- Transaction data: {total} operations processed"
        return (f"Transaction analysis: {total} operations, "
                f"net flow: {net_flows:+} units\n")

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        self.process_batch(data_batch)
        if criteria == "large":
            data = [f"{key}:{value}" for dic in data_batch
                    for key, value in dic.items()
                    if value > 150]
            total = len(data)
            if total != 0:
                self.filtered_data = f"{total} {criteria} transaction"
            else:
                self.filtered_data = None
            return data
        return []

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "type": self.type,
            "id": self.stream_id,
            "processed_data": self.processed_data,
            "result": self.message
                }


class EventStream(DataStream):
    def __init__(self, stream_id):
        self.stream_id = stream_id
        self.type = "System Events"
        self.alerts = []

    def process_batch(self, data_batch: List[Any]) -> str:
        errors = 0
        for i, string in enumerate(data_batch):
            if not isinstance(string, (str)):
                raise TypeError(f"'{string }' at index {i} must be a "
                                f"string, got {type(string).__name__}")
            if string == "":
                raise ValueError(f"value at index {i} cannot be empty")
            if string == "error":
                errors += 1
        total = len(data_batch)
        self.processed_data = f"Processing event batch: {data_batch}"
        self.message = f"- Event data: {total} events processed"
        return f"Event analysis: {total} events, {errors} error detected\n"

    def filter_data(self,
                    data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "type": self.type,
            "id": self.stream_id,
            "processed_data": self.processed_data,
            "result": self.message
                }


class StreamProcessor:
    def __init__(self):
        self.streams = []

    def add_stream(self, stream):
        self.streams.append(stream)

    def streams_manager(self, data: List[Any], print_it):
        try:
            for stream, data in zip(self.streams, data_batch):
                try:
                    message = stream.process_batch(data)
                    stream_infos = stream.get_stats()
                    if not isinstance(data, list):
                        raise TypeError(f"{data}: Invalid Respect This "
                                         "Format List[Dict]")
                except Exception as e:
                    print("=========================================")
                    print(f"{e.__class__.__name__}: {e}")
                    print("=========================================\n")
                    continue
                if print_it:
                    print(f"Stream ID: {stream_infos['id']}, "
                          f"Type: {stream_infos['type']}")
                    print(f"Initializing {stream_infos['type']} Stream...")
                    print(stream_infos['processed_data'])
                    print(message)
                    if stream.alerts != []:
                        print("ALERT!!:")
                        print(f"the values {stream.alerts} are extreme temperature values\n")
                else:
                    print(stream_infos['result'])
        except Exception as e:
            print(f"{e.__class__.__name__}: {e}\n")

    def Stream_filtering(self, data_batch, criteria):
        print("\nStream filtering active: High-priority data only")
        print("Filtered results: ", end="")
        i = 0
        for stream, data, cr in zip(self.streams, data_batch, criteria):
            stream.filter_data(data, cr)
            if i == 0:
                sep = ""
            else:
                sep = ", "
            if stream.filtered_data is not None:
                print(f"{sep}{stream.filtered_data}", end="")
            i += 1
        print()


if __name__ == "__main__":
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    streams = [SensorStream("SENSOR_001"), TransactionStream("TRANS_001"),
               EventStream("EVENT_001")]
    stream_processor = StreamProcessor()

    for stream in streams:
        stream_processor.add_stream(stream)

    data_batch = [
                [
                    {"temp": 22.5},
                    {"humidity": 75},
                    {"pressure": 1013}
                ],
                [
                    {"buy": 100},
                    {"sell": 150},
                    {"buy": 75}
                ],
                ["login", "error", "logout"]
            ]
    try:
        stream_processor.streams_manager(data_batch, True)
        print("=== Polymorphic Stream Processing ===")
        print("Processing mixed stream types through unified interface...\n")
        data_batch = [
                [
                    {"temp": 70.5},
                    {"humidity": 75}
                ],
                [
                    {"buy": 100},
                    {"sell": 150},
                    {"buy": 75},
                    {"sell": 200}
                ],
                ["login", "error", "logout"]
            ]
        print("Batch 1 Results:")
        stream_processor.streams_manager(data_batch, False)
        criteria = ["critical", "large", None]
        stream_processor.Stream_filtering(data_batch, criteria)
        print("\nAll streams processed successfully. "
              "Nexus throughput optimal.")

    except Exception as e:
        print(f"{e.__class__.__name__}: {e}\n")
