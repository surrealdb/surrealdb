"""
This file trains and saves the torch linear model to the model stash directory for the core to test against
"""
from tests.model_builder.utils import install_package
install_package("torch==2.1.2")
install_package("onnx==1.16.0")
import os

from surrealml.model_templates.torch.torch_linear import train_model as linear_torch_train_model
from surrealml.model_templates.torch.torch_linear import export_model_onnx as linear_torch_export_model_onnx
from surrealml.model_templates.torch.torch_linear import export_model_surml as linear_torch_export_model_surml

from tests.model_builder.utils import delete_directory, create_directory, MODEL_STASH_DIRECTORY


# create the model stash directory if it does not exist
create_directory(dir_path=MODEL_STASH_DIRECTORY)

torch_stash_directory = os.path.join(MODEL_STASH_DIRECTORY, "torch")
torch_surml_stash_directory = os.path.join(torch_stash_directory, "surml")
torch_onnx_stash_directory = os.path.join(torch_stash_directory, "onnx")

# delete the directories if they exist
delete_directory(dir_path=torch_stash_directory)
delete_directory(dir_path=torch_surml_stash_directory)
delete_directory(dir_path=torch_onnx_stash_directory)

# create directories for the torch models
create_directory(torch_stash_directory)
create_directory(torch_surml_stash_directory)
create_directory(torch_onnx_stash_directory)

# train and stash torch models
torch_linear_model, x = linear_torch_train_model()
torch_linear_surml_file = linear_torch_export_model_surml(torch_linear_model)
torch_linear_onnx_file = linear_torch_export_model_onnx(torch_linear_model)

torch_linear_surml_file.save(
    path=str(os.path.join(torch_surml_stash_directory, "linear.surml"))
)
