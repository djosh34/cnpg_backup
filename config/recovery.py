#!/usr/bin/env python3
"""Render a fresh recovery Cluster from a reviewed CNPG Cluster JSON template."""
import argparse
import json
from render import recovery_cluster

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster-file', required=True)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--name', required=True)
    parser.add_argument('--source-repository', required=True)
    parser.add_argument('--destination-repository', required=True)
    parser.add_argument('--target-json', default='{}', help='CNPG recoveryTarget object; {} means latest archived')
    args = parser.parse_args()
    with open(args.cluster_file) as stream:
        template = json.load(stream)
    print(json.dumps(recovery_cluster(template, args.namespace, args.name, args.source_repository,
                                     args.destination_repository, json.loads(args.target_json)), indent=2))
