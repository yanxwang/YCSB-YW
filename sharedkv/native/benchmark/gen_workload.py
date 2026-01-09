#!/usr/bin/env python3
"""
Generate YCSB workload files for SharedKV native benchmark.
Based on FUSEE's workload generation approach.
"""

import random
import sys
import os
import argparse

def generate_key(key_num):
    """Generate YCSB-style key: user{key_num}"""
    return f"user{key_num}"

def generate_value(num_fields=10, field_size=100):
    """Generate YCSB-style value: field0=xxx,field1=yyy,..."""
    fields = []
    for i in range(num_fields):
        # Generate random field value
        field_val = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=field_size))
        fields.append(f"field{i}={field_val}")
    return ','.join(fields)

def generate_workload_c(num_records, num_operations, output_dir):
    """
    Generate Workload C (100% READ)
    """
    print(f"Generating Workload C: {num_records} records, {num_operations} operations")

    os.makedirs(output_dir, exist_ok=True)

    load_file = os.path.join(output_dir, "workloadc_load.txt")
    trans_file = os.path.join(output_dir, "workloadc_trans.txt")

    # Generate load file (INSERT operations)
    print(f"Writing load file: {load_file}")
    with open(load_file, 'w') as f:
        for i in range(num_records):
            key = generate_key(i)
            value = generate_value()
            f.write(f"INSERT {key} {value}\n")

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i+1}/{num_records} load operations...")

    # Generate transaction file (READ operations with Zipfian distribution)
    print(f"Writing transaction file: {trans_file}")

    # Generate Zipfian distribution of keys
    # Use simple power-law distribution as approximation
    key_accesses = []
    for _ in range(num_operations):
        # Zipfian: more accesses to lower-numbered keys
        # Use power law: P(k) ~ k^(-alpha), alpha = 0.99 for YCSB
        key_num = int(random.paretovariate(1.5) * 100) % num_records
        key_accesses.append(key_num)

    with open(trans_file, 'w') as f:
        for i, key_num in enumerate(key_accesses):
            key = generate_key(key_num)
            f.write(f"READ {key}\n")

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i+1}/{num_operations} transaction operations...")

    print(f"Workload C generated successfully!")
    print(f"  Load file:        {load_file}")
    print(f"  Transaction file: {trans_file}")

def generate_workload_a(num_records, num_operations, output_dir):
    """
    Generate Workload A (50% READ, 50% UPDATE)
    """
    print(f"Generating Workload A: {num_records} records, {num_operations} operations")

    os.makedirs(output_dir, exist_ok=True)

    load_file = os.path.join(output_dir, "workloada_load.txt")
    trans_file = os.path.join(output_dir, "workloada_trans.txt")

    # Generate load file (INSERT operations)
    print(f"Writing load file: {load_file}")
    with open(load_file, 'w') as f:
        for i in range(num_records):
            key = generate_key(i)
            value = generate_value()
            f.write(f"INSERT {key} {value}\n")

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i+1}/{num_records} load operations...")

    # Generate transaction file (50% READ, 50% UPDATE)
    print(f"Writing transaction file: {trans_file}")

    with open(trans_file, 'w') as f:
        for i in range(num_operations):
            # Zipfian key selection
            key_num = int(random.paretovariate(1.5) * 100) % num_records
            key = generate_key(key_num)

            # 50/50 split between READ and UPDATE
            if random.random() < 0.5:
                f.write(f"READ {key}\n")
            else:
                value = generate_value()
                f.write(f"UPDATE {key} {value}\n")

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i+1}/{num_operations} transaction operations...")

    print(f"Workload A generated successfully!")
    print(f"  Load file:        {load_file}")
    print(f"  Transaction file: {trans_file}")

def generate_workload_b(num_records, num_operations, output_dir):
    """
    Generate Workload B (95% READ, 5% UPDATE)
    """
    print(f"Generating Workload B: {num_records} records, {num_operations} operations")

    os.makedirs(output_dir, exist_ok=True)

    load_file = os.path.join(output_dir, "workloadb_load.txt")
    trans_file = os.path.join(output_dir, "workloadb_trans.txt")

    # Generate load file
    print(f"Writing load file: {load_file}")
    with open(load_file, 'w') as f:
        for i in range(num_records):
            key = generate_key(i)
            value = generate_value()
            f.write(f"INSERT {key} {value}\n")

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i+1}/{num_records} load operations...")

    # Generate transaction file (95% READ, 5% UPDATE)
    print(f"Writing transaction file: {trans_file}")

    with open(trans_file, 'w') as f:
        for i in range(num_operations):
            key_num = int(random.paretovariate(1.5) * 100) % num_records
            key = generate_key(key_num)

            if random.random() < 0.95:
                f.write(f"READ {key}\n")
            else:
                value = generate_value()
                f.write(f"UPDATE {key} {value}\n")

            if (i + 1) % 10000 == 0:
                print(f"  Generated {i+1}/{num_operations} transaction operations...")

    print(f"Workload B generated successfully!")

def main():
    parser = argparse.ArgumentParser(description='Generate YCSB workload files for SharedKV')
    parser.add_argument('-w', '--workload', required=True, choices=['a', 'b', 'c'],
                       help='Workload type (a, b, or c)')
    parser.add_argument('-r', '--records', type=int, default=100000,
                       help='Number of records to load (default: 100000)')
    parser.add_argument('-o', '--operations', type=int, default=1000000,
                       help='Number of operations in transaction phase (default: 1000000)')
    parser.add_argument('-d', '--output-dir', default='benchmark/workloads',
                       help='Output directory (default: benchmark/workloads)')

    args = parser.parse_args()

    print("=" * 60)
    print("YCSB Workload Generator for SharedKV")
    print("=" * 60)

    if args.workload == 'c':
        generate_workload_c(args.records, args.operations, args.output_dir)
    elif args.workload == 'a':
        generate_workload_a(args.records, args.operations, args.output_dir)
    elif args.workload == 'b':
        generate_workload_b(args.records, args.operations, args.output_dir)

    print("=" * 60)
    print("Done!")
    print("=" * 60)

if __name__ == '__main__':
    main()
