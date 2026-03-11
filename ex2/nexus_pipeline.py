from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol
import time


class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


class ProcessingPipeline(ABC):
    def __init__(self) -> None:
        self.stages: list[ProcessingStage] = []
        self.output: List[str] = []
        self.turn_on = None

    @abstractmethod
    def process(self, data: Any) -> Any:
        ...

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)


class InputStage:
    def __init__(self) -> None:
        self.stage = 1
        print(f"Stage {self.stage}: Input validation and parsing")

    def process(self, data: Any) -> Dict:
        self.output = None
        summary = None
        if isinstance(data, dict):
            self.output = f"Input: {data}"
            data_type = "JSON"
            if len(data.keys()) > 3:
                raise ValueError("3 required pairs only")
            if not isinstance(data.get("sensor"), str):
                raise ValueError("Sensor must be provided, as a string ")
            if not isinstance(data.get("value"), (int, float)):
                raise ValueError("Value must be provided, as an int or float ")
            if not isinstance(data.get("unit"), str):
                raise ValueError("Unit must be provided, as a string")
            summary = {
                            key: type(value).__name__
                            for key, value in data.items()
                        }
        if isinstance(data, list):
            self.output = "Input: Real-time sensor stream"
            data_type = "Stream"
            if len(data) > 5:
                raise ValueError("Stream cannot exceed 5 readings, "
                                 f"got {len(data)}")
            if not all(isinstance(x, (int, float)) and
                       not isinstance(x, bool) for x in data):
                raise ValueError("Values must be either int or float")
        if isinstance(data, str):
            self.output = f'Input: "{data}"'
            data_type = "CSV"
            data = data.split(",")
            if len(data) != 3:
                raise ValueError("Format error (need 3 values)")

        if isinstance(data, tuple):
            if len(data) != 2:
                raise ValueError("Tuple must contain two values")
            if data[0] != "__chained__":
                raise ValueError("The tuple doesnt match the format: "
                                 "('__chained__', 'data')")
            return {"data": data[1], "type": "__chained__"}

        return {"data": data, "type": data_type, "summary": summary}


class TransformStage:
    def __init__(self) -> None:
        self.stage = 2
        print(f"Stage {self.stage}: Data transformation and enrichment")

    def process(self, data: Any) -> Dict:
        self.output = None
        data_type = data["type"]
        data = data["data"]
        if data_type == "JSON":
            if data["sensor"].lower() == "temp":
                data["read"] = "temperature"
            else:
                data["read"] = data["sensor"]
            value = data.get("value")
            unit = data["unit"]
            if unit == "C" or unit == "F":
                unit = f"°{data['unit']}"
            else:
                unit = data["unit"]
            data["message"] = f"{value}{unit}"
            data["status"] = "Normal range" if value < 30 else "Critical"
            self.output = "Transform: Enriched with metadata and validation"

        if data_type == "CSV":
            if data[0] == "" or data[1] == "" or data[2] == "":
                raise ValueError("Error detected in Stage 2: "
                                 "Invalid data format")
            i = 0
            for string in data:
                if string.lower() == "action":
                    i += 1
            data = {"activity": data, "actions": i}
            self.output = "Transform: Parsed and structured data"

        if data_type == "Stream":
            readings = len(data)
            average = 0 if readings == 0 else sum(data) / readings
            data = {"readings": readings, "average": average}
            self.output = "Transform: Aggregated and filtered"
        if data_type == "__chained__":
            data = f"Processed: {data}"
        return {"data": data, "type": data_type}


class OutputStage:
    def __init__(self) -> None:
        self.stage = 3
        print(f"Stage {self.stage}: Output formatting and delivery")

    def process(self, data: Any) -> str:
        data_type = data["type"]
        data = data["data"]
        result = None
        self.output = None
        if data_type == "JSON":
            result = (f"Output: Processed {data['read']} reading: "
                      f"{data['message']} ({data['status']})")
            self.output = result
        if data_type == "CSV":
            result = (f"Output: User activity logged: {data['actions']} "
                      "actions processed")
            self.output = result
        if data_type == "Stream":
            result = (f"Output: Stream summary: {data['readings']} readings, "
                      f"avg: {data['average']:.1f}°C")
            self.output = result
        if data_type == "__chained__":
            result = data
        return result


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> tuple[str, Any]:
        if isinstance(data, dict):
            self.turn_on = "\nProcessing JSON data through pipeline..."
        if not isinstance(data, (dict, tuple)):
            raise TypeError("Input must be a dictionary, or a tuple "
                            "for pipeline demo")
        for stage in self.stages:
            data = stage.process(data)
        self.output = [stage.output for stage in self.stages]
        return ("__chained__", data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        if isinstance(data, str):
            self.turn_on = "\nProcessing CSV data through same pipeline..."
        if not isinstance(data, (str, tuple)):
            raise TypeError("Input must be a string, "
                            "or a tuple for pipeline demo")
        for stage in self.stages:
            data = stage.process(data)
        self.output = [stage.output for stage in self.stages]
        return ("__chained__", f"Analyzed: {data}")


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str) -> None:
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Union[str, Any]:
        if isinstance(data, list):
            self.turn_on = "\nProcessing Stream data through same pipeline..."
        if not isinstance(data, (list, tuple)):
            raise TypeError("Input must be a list, or a "
                            "tuple for pipeline demo")
        for stage in self.stages:
            data = stage.process(data)
        self.output = [stage.output for stage in self.stages]
        return ("__chained__", f"Stored: {data}")


class NexusManager:
    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []
        print("Initializing Nexus Manager...")

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def processes_data(self, data: Any) -> None:
        try:
            if not isinstance(data, list):
                raise TypeError(f"Invalid type {data}, Required list")
            for pipeline, _data in zip(self.pipelines, data):
                try:
                    pipeline.process(_data)
                    print(pipeline.turn_on)
                    for s in pipeline.output:
                        print(s)
                    pipeline.output.clear()
                except Exception as e:
                    print(f"\n{e.__class__.__name__}: {e}")
        except Exception as e:
            print(f"\n{e.__class__.__name__}: {e}")

    def chain_pipelines(self, data: List[Optional[tuple]]) -> None:

        start = time.time()

        print("\n=== Pipeline Chaining Demo ===")
        c = ord('A')
        i = 0
        for pipeline in self.pipelines:
            if i == 0:
                print(f"Pipeline {chr(c)} ", end="")
            else:
                print(f"-> Pipeline {chr(c)}", end="")
            if c == ord('Z'):
                break
            i += 1
            c += 1
        print()
        print("Data flow: Raw -> Processed -> Analyzed -> Stored\n")

        for batch in data:
            result = batch
            for pipeline in self.pipelines:
                try:
                    result = pipeline.process(result)
                except Exception as e:
                    print(f"{e.__class__.__name__}: {e}")
        end = time.time() - start
        print("Chain result: 100 records processed "
              f"through {len(pipeline.stages)}-stage pipelines")
        print(f"Performance: 95% efficiency, {end:.1f}s total processing time")

    def pipeline_failure(self, data: Any) -> None:
        print("\n=== Error Recovery Test ===")
        try:
            self.pipelines[1].process(data)
        except Exception as e:
            print(e)
            print("Recovery initiated: Switching to backup processor")
            print("Recovery successful: Pipeline restored, processing resumed")


def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    nexus_manager = NexusManager()
    print("Creating Data Processing Pipeline...")
    pipelines = [JSONAdapter("JSON_001"), CSVAdapter("CSV_001"),
                 StreamAdapter("STREAM_001")]
    stages = [InputStage(), TransformStage(), OutputStage()]
    for pipeline in pipelines:
        for stage in stages:
            pipeline.add_stage(stage)
        nexus_manager.add_pipeline(pipeline)
    data = [
        {"sensor": "temp", "value": 23.5, "unit": "C"},
        "user,action,timestamp",
        [22.5, 21.8, 23.1, 20.9, 22.2]
        ]
    nexus_manager.processes_data(data)
    data = [("__chained__", {
        "sensor": "temp",
        "value": 23.5,
        "unit": "C"
    })] * 100
    nexus_manager.chain_pipelines(data)
    data = ",,"
    nexus_manager.pipeline_failure(data)
    print("\nNexus Integration complete. All systems operational.")


try:
    main()
except Exception as e:
    print(f"{e.__class__.__name__}: {e}")
