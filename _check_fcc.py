import json, glob, os

files = sorted(glob.glob("data/**/*_info.json", recursive=True))
has_fcc = []
no_fcc = []
for f in files:
    with open(f) as fh:
        d = json.load(fh)
    label = d.get("sortie", os.path.basename(f))
    ver = d.get("fcc_version")
    if ver:
        has_fcc.append((label, ver))
    else:
        no_fcc.append(label)

print(f"With fcc_version ({len(has_fcc)}):")
for label, ver in has_fcc:
    print(f"  {label}: {ver}")
print(f"\nMissing fcc_version ({len(no_fcc)}):")
for label in no_fcc:
    print(f"  {label}")
