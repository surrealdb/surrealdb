"""
Trains and exports models to be used for testing.
"""
import os
import sys

import onnx

script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)

tests_directory = os.path.join(script_directory, "..")
main_directory = os.path.join(script_directory, "..", "..")

# Add a directory to the PYTHONPATH
sys.path.append(main_directory)


import shutil
from surrealml.model_templates.sklearn.sklearn_linear import train_model as linear_sklearn_train_model
from surrealml.model_templates.sklearn.sklearn_linear import export_model_onnx as linear_sklearn_export_model_onnx
from surrealml.model_templates.sklearn.sklearn_linear import export_model_surml as linear_sklearn_export_model_surml

from surrealml.model_templates.onnx.onnx_linear import train_model as linear_onnx_train_model
from surrealml.model_templates.onnx.onnx_linear import export_model_onnx as linear_onnx_export_model_onnx
from surrealml.model_templates.onnx.onnx_linear import export_model_surml as linear_onnx_export_model_surml

from surrealml.model_templates.torch.torch_linear import train_model as linear_torch_train_model
from surrealml.model_templates.torch.torch_linear import export_model_onnx as linear_torch_export_model_onnx
from surrealml.model_templates.torch.torch_linear import export_model_surml as linear_torch_export_model_surml

from surrealml.model_templates.tensorflow.tensorflow_linear import train_model as linear_tensorflow_train_model
from surrealml.model_templates.tensorflow.tensorflow_linear import export_model_onnx as linear_tensorflow_export_model_onnx
from surrealml.model_templates.tensorflow.tensorflow_linear import export_model_surml as linear_tensorflow_export_model_surml


def delete_directory(dir_path: os.path) -> None:
    """
    Checks to see if a directory exists and deletes it if it does.

    :param dir_path: the path to the directory.
    """
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
        print(f"Directory '{dir_path}' has been deleted.")
    else:
        print(f"Directory '{dir_path}' does not exist.")


def delete_file(file_path: os.path) -> None:
    """
    Checks to see if a file exists and deletes it if it does.

    :param file_path: the path to the file.
    """
    if os.path.isfile(file_path):
        os.remove(file_path)
        print(f"File '{file_path}' has been deleted.")
    else:
        print(f"File '{file_path}' does not exist.")


def write_file(file_path: os.path, model, file_name) -> None:
    """
    Writes a file to the specified path.

    :param file_path: the path to write the file to.
    :param model: the model to write to the file.
    :param file_name: the name of the file to write.
    """
    with open(os.path.join(file_path, file_name), "wb") as f:
        f.write(model)

core_directory = os.path.join(main_directory, "modules", "core")

model_stash_directory = os.path.join(core_directory, "model_stash")

# defining directories for sklearn
sklearn_stash_directory = os.path.join(model_stash_directory, "sklearn")
sklearn_surml_stash_directory = os.path.join(sklearn_stash_directory, "surml")
sklearn_onnx_stash_directory = os.path.join(sklearn_stash_directory, "onnx")

# defining directories for onnx
onnx_stash_directory = os.path.join(model_stash_directory, "onnx")
onnx_surml_stash_directory = os.path.join(onnx_stash_directory, "surml")
onnx_onnx_stash_directory = os.path.join(onnx_stash_directory, "onnx")

# defining directories for torch
torch_stash_directory = os.path.join(model_stash_directory, "torch")
torch_surml_stash_directory = os.path.join(torch_stash_directory, "surml")
torch_onnx_stash_directory = os.path.join(torch_stash_directory, "onnx")

# defining directories for tensorflow
tensorflow_stash_directory = os.path.join(model_stash_directory, "tensorflow")
tensorflow_surml_stash_directory = os.path.join(tensorflow_stash_directory, "surml")
tensorflow_onnx_stash_directory = os.path.join(tensorflow_stash_directory, "onnx")


target_directory = os.path.join(main_directory, "target")
egg_info_dir = os.path.join(main_directory, "surrealml.egg-info")


def main():
    print("main running")
    # wipe and create directories for model stashes
    delete_directory(model_stash_directory)

    os.mkdir(model_stash_directory)

    # create directories for the different model types
    os.mkdir(sklearn_stash_directory)
    os.mkdir(sklearn_surml_stash_directory)
    os.mkdir(sklearn_onnx_stash_directory)

    os.mkdir(onnx_stash_directory)
    os.mkdir(onnx_surml_stash_directory)
    os.mkdir(onnx_onnx_stash_directory)

    os.mkdir(torch_stash_directory)
    os.mkdir(torch_surml_stash_directory)
    os.mkdir(torch_onnx_stash_directory)

    os.mkdir(tensorflow_stash_directory)
    os.mkdir(tensorflow_surml_stash_directory)
    os.mkdir(tensorflow_onnx_stash_directory)

    # train and stash sklearn models
    sklearn_linear_model = linear_sklearn_train_model()
    sklearn_linear_surml_file = linear_sklearn_export_model_surml(sklearn_linear_model)
    sklearn_linear_onnx_file = linear_sklearn_export_model_onnx(sklearn_linear_model)

    sklearn_linear_surml_file.save(
        path=str(os.path.join(sklearn_surml_stash_directory, "linear.surml"))
    )
    onnx.save(
        sklearn_linear_onnx_file,
        os.path.join(sklearn_onnx_stash_directory, "linear.onnx")
    )

    # train and stash onnx models
    onnx_linear_model = linear_onnx_train_model()
    onnx_linear_surml_file = linear_onnx_export_model_surml(onnx_linear_model)
    onnx_linear_onnx_file = linear_onnx_export_model_onnx(onnx_linear_model)

    onnx_linear_surml_file.save(
        path=str(os.path.join(onnx_surml_stash_directory, "linear.surml"))
    )
    onnx.save(
        onnx_linear_onnx_file,
        os.path.join(onnx_onnx_stash_directory, "linear.onnx")
    )

    # train and stash torch models
    torch_linear_model, x = linear_torch_train_model()
    torch_linear_surml_file = linear_torch_export_model_surml(torch_linear_model)
    torch_linear_onnx_file = linear_torch_export_model_onnx(torch_linear_model)

    torch_linear_surml_file.save(
        path=str(os.path.join(torch_surml_stash_directory, "linear.surml"))
    )
    # onnx.save(
    #     torch_linear_onnx_file,
    #     os.path.join(torch_onnx_stash_directory, "linear.onnx")
    # )

    # train and stash tensorflow models
    tensorflow_linear_model = linear_tensorflow_train_model()
    tensorflow_linear_surml_file = linear_tensorflow_export_model_surml(tensorflow_linear_model)
    tensorflow_linear_onnx_file = linear_tensorflow_export_model_onnx(tensorflow_linear_model)

    tensorflow_linear_surml_file.save(
        path=str(os.path.join(tensorflow_surml_stash_directory, "linear.surml"))
    )

    os.system(f"cd {model_stash_directory} && tree")

    shutil.rmtree(".surmlcache")


if __name__ == '__main__':
    main()
