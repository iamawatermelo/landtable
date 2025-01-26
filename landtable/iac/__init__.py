"""
The Landtable IaC CLI.
"""

import json
import os
from pathlib import Path
from time import sleep
from typing import Annotated
import typer
from rich.progress import Progress, TextColumn, SpinnerColumn
from cuddly_dicts import kdl_source_to_dict

iac = typer.Typer()


def preprocess(in_dict: dict, env: str | None, path: Path) -> dict:
    """
    Resolve directives like @apply and @env.
    """
    
    out_dict = dict()
    
    for k, v in in_dict.items():
        if k == "@apply":
            assert isinstance(v, str) or isinstance(v, list), "@apply directive expects a string or a list"
            
            if isinstance(v, str):
                v = [v]
            
            for apply_path in v:
                with open(path / apply_path) as file:
                    out_dict = {
                        **out_dict,
                        **preprocess(
                            kdl_source_to_dict(file.read()),
                            env,
                            (path / apply_path).parent
                        )
                    }
        elif k == "@env":
            assert isinstance(v, dict), "@env directive expects a dict"
            
            if (apply := v.get(env)):
                out_dict = {
                    **out_dict,
                    **preprocess(apply, env, path)
                }
        elif isinstance(v, dict):
            out_dict[k] = preprocess(v, env, path)
        else:
            out_dict[k] = v
        
    return out_dict

@iac.command()
def apply(
    meta: Annotated[Path, typer.Option("--meta")],
    files: list[Path],
    env: Annotated[str | None, typer.Option("--env", "-e")] = None
):
    with Progress(
        SpinnerColumn(),
        TextColumn("[pink][progress.description]{task.description}"),
    ) as progress:
        task1 = progress.add_task("Loading configuration files...", total=1)
        
        with open(meta) as meta_file:
            meta_dict = kdl_source_to_dict(meta_file.read())
        
        if env is None:
            env = os.environ.get("LANDTABLE_IAC_ENV")
        
        meta_dict = preprocess(meta_dict, env, meta.parent)
        
        progress.update(
            task1,
            visible=False
        )
        progress.console.log("Loaded configuration files")
        
        