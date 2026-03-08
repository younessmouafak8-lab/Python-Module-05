from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol



class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...

class ProcessingPipeline(ABC):
    def __init__(self):
        self.stages: list[ProcessingStage] = []

    @abstractmethod
    def process(self, data: Any) -> Any:
        ...

    def add_stage(self, stage: ProcessingStage):
        self.stages.append(stage)


class InputStage:
    def __init__(self):
        print("Stage 1: Input validation and parsing")

    def process(self, data: Any) -> Dict:
        if isinstance(data, dict):
            print(f"Input: {data}")
            data_type = "JSON"
            if len(data.keys()) > 3:
                raise ValueError("3 required pairs only")
            if not isinstance(data.get("sensor"), str):
                raise ValueError("Sensor must be provided, as a string ")
            if not isinstance(data.get("value"), (int, float)):
                raise ValueError("Value must be provided, as an int or float ")
            if not isinstance(data.get("unit"), str):
                raise ValueError("Unit must be provided, as a string")
        if isinstance(data, list):
            print(f"Input: Real-time sensor stream")
            data_type = "Stream"
            if len(data) > 5:
                raise ValueError(f"Stream cannot exceed 5 readings, got {len(data)}")
            if not all(isinstance(x, (int, float)) and
                       not isinstance(x, bool) for x in data):
                raise ValueError("Values must be either int or float")
            
        if isinstance(data, str):
            print(f'Input: "{data}"')
            data_type = "CSV"
            data = data.split(",")
            if len(data) != 3:
                raise ValueError("Format error (need 3 values)")
            if data[0] == "" or data[1] == "" or data[2] == "":
                raise ValueError("Empty Values will not be tolerated")

        return {"data": data, "type": data_type}


class TransformStage:
    def __init__(self):
        print("Stage 2: Data transformation and enrichment")

    def process(self, data: Any) -> Dict:
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
            print("Transform: Enriched with metadata and validation")

        if data_type == "CSV":
            i = 0
            for string in data:
                if string.lower() == "action":
                    i += 1
            data = {"activity": data, "actions": i}

        if data_type == "Stream":
            readings = len(data)
            if readings == 0:
                average = 0
            else:
                average = sum(data) / readings
            data = {"readings": readings, "average": average}
            print("Transform: Aggregated and filtered")
        return {"data": data, "type": data_type}


class OutputStage:
    def __init__(self):
        print("Stage 3: Output formatting and delivery")

    def process(self, data: Any) -> str:
        data_type = data["type"]
        data = data["data"]
        if data_type == "JSON":
            print(f"Output: Processed {data['read']} reading: {data['message']} ({data['status']})")
        if data_type == "CSV":
            print(f"Output: User activity logged: {data['actions']} actions processed")
        if data_type == "Stream":
            print(f"Output: Stream summary: {data['readings']} readings, avg: {data['average']:.1f}°C")


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        print("\nProcessing JSON data through pipeline...")
        if not isinstance(data, dict):
                raise TypeError(f"Input must be a dictionary")
        for stage in self.stages:
            data = stage.process(data)


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        print("\nProcessing CSV data through same pipeline...")
        if not isinstance(data, str):
            raise TypeError(f"Input must be a string")
        for stage in self.stages:
            data = stage.process(data)

class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Any:
        print("\nProcessing Stream data through same pipeline...")
        if not isinstance(data, list):
            raise TypeError(f"Input must be a list")
        for stage in self.stages:
            data = stage.process(data)


class NexusManager:
    def __init__(self):
        self.pipelines: list[ProcessingPipeline] = []
        print("Initializing Nexus Manager...")

    def add_pipeline(self, pipeline):
        self.pipelines.append(pipeline)

    def processes_data(self, data: Any) -> Any:
        for pipeline, _data in zip(self.pipelines, data):
            pipeline.process(_data)

def main():
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    nexus_manager = NexusManager()
    print("Creating Data Processing Pipeline...")
    pipelines = [JSONAdapter("JSON_001"), CSVAdapter("CSV_001"), StreamAdapter("STREAM_001")]
    stages = [InputStage(), TransformStage(), OutputStage()]
    for pipeline in pipelines:
        for stage in stages:
            pipeline.add_stage(stage)
        nexus_manager.add_pipeline(pipeline)
    # data =  {"sensor": "temp", "value": 23.5, "unit": "C"}
    # data = "user,action,timestamp,action"
    data = [
        {"sensor": "temp", "value": 23.5, "unit": "C"},
        "user,action,timestamp",
        [22.5, 21.8, 23.1, 20.9, 22.2]
        ]
    nexus_manager.processes_data(data)
    


main()
