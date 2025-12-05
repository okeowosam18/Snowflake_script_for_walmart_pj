import os

print("Files in output directory:")
if os.path.exists('output'):
    files = os.listdir('output')
    print(f"Found {len(files)} files:")
    for f in files:
        size = os.path.getsize(f'output/{f}')
        print(f"  - {f} ({size:,} bytes)")
else:
    print("Output directory doesn't exist!")