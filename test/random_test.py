#! /usr/bin/python
#
# Tests the approximate triangle counting code by generating a random graph
# with a given number of nodes and then comparing the output of the approx. 
# code with a stable, exact solution in igraph.

import re
import os
import csv
import math
import random
import shutil
import igraph
import argparse

# Global constants (executable/script locations)
approxTriangleJar = "../target/scala-2.10/approxtrianglecounting_2.10-1.0.jar"
errorFigScript = "clustering_coeff_dist.R"
exactTriangleScript = "../src/main/R/serial_triangle_counting.R"

# Command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("nodes", help="number of nodes in the random graph", type=int)
args = parser.parse_args()
N = args.nodes


def log(s):
    print"[TEST] {}".format(s)

def forest_fire(N):
    p = random.uniform(0.01, 0.3)
    log("Generating a Forest Fire random graph with parameters: N = {}, P = {}".format(N,p))
    return igraph.Graph.Forest_Fire(N, p)

def barabasi(N):
    m = random.randrange(1, int(math.sqrt(N)))  # number of edges per node, try to keep things sparse
    log("Generating a Barabasi-Albert random graph with parameters: N = {}, M = {}".format(N,m))
    return igraph.Graph.Barabasi(N,m)

def erdos_renyi(N):
    p = random.random()/10.
    log("Generating a Erdos-Renyi graph with parameters: N = {}, p = {}".format(N, p))
    return igraph.Graph.Erdos_Renyi(N,p)

def full(N):
    N = random.randrange(1, 100)
    log("Generating a fully connected graph with parameters: N = {}".format(N))
    return igraph.Graph.Full(N)

def growing_random(N):
    m = random.randrange(1, 20)
    log("Generating a Growing Random graph with parameters: N = {}, M = {}".format(N,m))
    return igraph.Graph.Growing_Random(N,m)

def star(N):
    log("Generating a Star graph with parameters: N = {}".format(N))
    return igraph.Graph.Star(N)

def random_graph(graph_func, N, filename):
    g = graph_func(N)
    g = g.simplify()
    graphFile = filename
    with open(graphFile, "w") as f:
        for edge in g.get_edgelist():
            f.write("{},{}\n".format(edge[0], edge[1]))

def run_distributed_count(infile, outspark, outbins):
    cmd = "spark-submit --class Driver --jars ../src/main/scala/guava-11.0.jar  --executor-memory=5g   {} {} > {}".format(approxTriangleJar, infile, outspark)
    os.system(cmd)
    log("Finished running the spark job")
    with open(outspark, "r") as f:
        with open(outbins, "w") as g:
            writer = csv.writer(g)
            text = [line for line in f]
            writer.writerow(('degree','coeff','error'))
            for line in text:
                if line.startswith('Bin'):
                    stuff = line.split()
                    writer.writerow((stuff[1], stuff[5],stuff[-1]))

def run_exact_count(infile, outbins):
    cmd = "{} {}".format(exactTriangleScript, infile)
    os.system(cmd)
    log("Finished running igraph via R")
    shutil.move("exact.csv", outbins)

def generate_error_graph(spark_result, exact_result, error_figure_name):
    cmd = "{} {} {}".format(errorFigScript, exact_result, spark_result)
    os.system(cmd)
    os.remove("Rplots.pdf")
    shutil.move("error_figure.png", error_figure_name)


# -- main method --

# Generate a directory to store the results
log("generating a random graph")
try:
    shutil.rmtree("tests")
except Exception:
    pass
os.mkdir("tests")

graph_types = {"forest_fire":forest_fire, "barabasi":barabasi, "erdos_renyi":erdos_renyi,
               "full":full, "growing_random":growing_random, "star":star}

for name, func in graph_types.iteritems():
    spark_file = "tests/{}_log".format(name)
    spark_csv = "tests/{}_estimated.csv".format(name)
    graph_file = "tests/{}_edges".format(name)
    exact_file = "tests/{}_exact".format(name)
    fig_file = "tests/{}.png".format(name)

    random_graph(func, N, graph_file)
    run_distributed_count(graph_file, spark_file, spark_csv)
    run_exact_count(graph_file, exact_file)
    generate_error_graph(spark_csv, exact_file, fig_file)
