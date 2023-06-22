# -*- coding:utf-8 -*-
'''
Author: xupingmao xupingmao@gmail.com
Date: 2023-03-11 13:44:52
LastEditors: xupingmao
LastEditTime: 2023-06-22 15:06:05
FilePath: \bkv\setup.py
Description: A simple kv store
'''
import setuptools

with open("README.md", "r+", encoding="utf-8") as fp:
    long_description = fp.read()

setuptools.setup(
    name = "bkv",
    version = "0.0.1",
    author = "mark",
    author_email = "578749341@qq.com",
    description  = "A simple kv store",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/xupingmao/bkv",
    packages = setuptools.find_packages(),
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires = []
)
