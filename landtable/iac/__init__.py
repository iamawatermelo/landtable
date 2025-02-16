"""
The Landtable IaC CLI.
"""

import json
import os
from pathlib import Path
from time import sleep
from typing import Annotated
import typer
from rich.progress import Progress, TextColumn, SpinnerColumn, track
from cuddly_dicts import kdl_source_to_dict

from landtable.iac.config.workspace import WorkspaceDocumentV1

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
    recipes: list[Path],
    env: Annotated[str | None, typer.Option("--env", "-e")] = None
):  
    with Progress(
        SpinnerColumn(),
        TextColumn("[pink][progress.description]{task.description}"),
    ) as progress:
        resolved_recipes = list()
        config_task = progress.add_task("Loading configuration files...", total=len(recipes))
        
        for recipe in recipes:
            progress.update(config_task, advance=1)
            with open(recipe) as recipe_file:
                resolved_recipes.append(WorkspaceDocumentV1(
                    **kdl_source_to_dict(recipe_file.read())
                ))  
        
        progress.console.log("Loaded configuration files")
