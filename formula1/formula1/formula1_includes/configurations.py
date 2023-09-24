# Databricks notebook source
# folders paths, so we don't have to change the paths in each and every notebook, when the environment is changed. Only changes in this notebook can be done. 

raw_folder_path = 'abfss://raw@dbprojectformula1.dfs.core.windows.net'
processed_folder_path = 'abfss://processed@dbprojectformula1.dfs.core.windows.net'
presentation_folder_path = 'abfss://presentation@dbprojectformula1.dfs.core.windows.net'