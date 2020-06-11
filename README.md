# metaflow_example

Simple example of using metaflow in a datascience context.

We wanna show it's easy to go from jupyter notebooks to a metaflow pipeline

## Commands


+ `python main.py output-dot | dot -Tpng -o graph.png`

Builds an image from the graph (requires `dot`)

+ `python main.py show`

Shows info about the pipeline

+ `python main.py run`

Runs the whole pipeline