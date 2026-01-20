from surrealml import SurMlFile, Engine


new_file = SurMlFile.load("./linear.surml", engine=Engine.PYTORCH)

print(new_file.buffered_compute({
    "squarefoot": 1.0,
    "num_floors": 2.0
}))
