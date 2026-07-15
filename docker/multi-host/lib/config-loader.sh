#!/usr/bin/env bash
# ============================================================================
# Shared YAML config loader for Qonductor multi-host scripts.
#
# Parses docker/multi-host/cluster-config.yaml WITHOUT requiring PyYAML.
# Uses an embedded Python stdlib-only YAML parser (same technique as
# docker/example.sh) that outputs bash-eval'able variable assignments.
#
# Usage:
#   source "${SCRIPT_DIR}/lib/config-loader.sh"
#   load_cluster_config [path/to/cluster-config.yaml]
#
# After calling load_cluster_config, the following variables are available:
#
#   CLUSTER_NAME, CLUSTER_KUBERNETESVERSION, CLUSTER_K3STOKEN,
#   CLUSTER_FLANNELINTERFACE
#
#   SERVER_HOST, SERVER_CONTAINERNAME, SERVER_NODENAME, SERVER_NODETYPE
#
#   AGENT_COUNT
#   AGENT_0_HOST, AGENT_0_CONTAINERNAME, AGENT_0_NODENAME, AGENT_0_NODETYPE
#   AGENT_0_QPUS=(...)           # bash indexed array of QPU filenames
#   AGENT_0_QPU_COUNT
#   ... (same for AGENT_1_*, AGENT_2_*, etc.)
# ============================================================================

# ---- Embedded Python YAML parser (stdlib only, no PyYAML needed) ----
PY_YAML_PARSER=$(cat <<'PY_YAML'
import sys, re

def strip_comment(line):
    """Remove inline YAML comment, respecting quoted strings."""
    in_single = False
    in_double = False
    for i, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == '#' and not in_single and not in_double:
            return line[:i].rstrip()
    return line


def parse_scalar(val):
    """Parse a scalar YAML value to its Python type."""
    val = val.strip()
    if not val:
        return None
    if (val.startswith('"') and val.endswith('"')) or \
       (val.startswith("'") and val.endswith("'")):
        return val[1:-1]
    vl = val.lower()
    if vl == 'true':   return True
    if vl == 'false':  return False
    if vl in ('null', '~'): return None
    if vl == '[]':     return []
    try:
        if '.' in val: return float(val)
        return int(val)
    except ValueError:
        pass
    return val


def set_nested(d, path, val):
    """Set a value at a dotted path, creating intermediate containers.
    When a numeric index follows a key whose placeholder is a dict,
    replaces the placeholder with a list automatically."""
    keys = [k for k in path.split('.') if k]
    if not keys:
        return
    for i, k in enumerate(keys[:-1]):
        kk = int(k) if k.isdigit() else k
        next_is_idx = keys[i + 1].isdigit()
        if isinstance(d, list):
            while len(d) <= kk:
                d.append({} if not next_is_idx else [])
            d = d[kk]
        else:
            default = [] if next_is_idx else {}
            existing = d.get(kk)
            if existing is not None and next_is_idx and not isinstance(existing, list):
                d[kk] = []  # replace placeholder dict with list
            d = d.setdefault(kk, default)
    last = keys[-1]
    last = int(last) if last.isdigit() else last
    if isinstance(d, list):
        while len(d) <= last:
            d.append(None)
    d[last] = val


def normalize(obj):
    """Convert dicts whose keys are all integers into proper lists."""
    if isinstance(obj, dict):
        if obj:
            keys_are_ints = all(
                (isinstance(k, int)) or (isinstance(k, str) and k.isdigit())
                for k in obj.keys()
            )
            if keys_are_ints:
                max_idx = max(int(k) for k in obj.keys())
                lst = [None] * (max_idx + 1)
                for k, v in obj.items():
                    lst[int(k)] = normalize(v)
                return lst
        return {k: normalize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [normalize(item) for item in obj]
    return obj


def parse_yaml(path):
    """Parse a YAML file into nested Python dicts/lists (no PyYAML)."""
    with open(path) as f:
        lines = f.readlines()

    raw = []
    for line in lines:
        stripped = line.rstrip()
        if not stripped or stripped.lstrip().startswith('#'):
            continue
        cleaned = strip_comment(stripped)
        if not cleaned or cleaned.lstrip().startswith('#'):
            continue
        raw.append(cleaned)

    result = {}
    stack = []   # (indent, path_prefix)

    for line in raw:
        content = line.lstrip()
        indent = len(line) - len(content)

        while stack and stack[-1][0] >= indent:
            stack.pop()
        parent_path = stack[-1][1] if stack else ''

        if content.startswith('- '):
            value = content[2:].strip()
            lst_path = parent_path
            obj = result
            for k in lst_path.split('.'):
                if not k:
                    continue
                kk = int(k) if k.isdigit() else k
                # get-or-create the container at this level
                if isinstance(obj, list):
                    if kk >= len(obj):
                        obj.append({})
                    obj = obj[kk]
                else:
                    obj = obj.setdefault(kk, {})
            idx = len(obj) if isinstance(obj, list) else 0
            cur_path = f"{lst_path}.{idx}" if lst_path else str(idx)

            if ':' in value:
                k, v = value.split(':', 1)
                k, v = k.strip(), v.strip()
                set_nested(result, cur_path, {})
                if v:
                    set_nested(result, f"{cur_path}.{k}", parse_scalar(v))
                else:
                    set_nested(result, f"{cur_path}.{k}", None)
            elif value:
                set_nested(result, cur_path, parse_scalar(value))
            else:
                set_nested(result, cur_path, {})
            stack.append((indent, cur_path))

        elif ':' in content:
            key, _, val = content.partition(':')
            key = key.strip()
            val = val.strip()
            cur_path = f"{parent_path}.{key}" if parent_path else key

            if val:
                set_nested(result, cur_path, parse_scalar(val))
            else:
                set_nested(result, cur_path, {})
                stack.append((indent, cur_path))

    return normalize(result)


def sh_quote(val):
    """Quote a Python value for safe bash eval via single-quote assignment."""
    if val is None:
        return "''"
    if isinstance(val, bool):
        return 'true' if val else 'false'
    if isinstance(val, (int, float)):
        # Use repr to preserve full precision on floats
        return repr(val)
    s = str(val)
    return "'" + s.replace("'", "'\\''") + "'"


def emit_bash_vars(data, prefix=''):
    """Walk the parsed YAML tree and print bash variable assignments."""
    if not isinstance(data, dict):
        return

    for key, val in data.items():
        var_prefix = f"{prefix}{key.upper()}" if prefix else key.upper()

        if isinstance(val, dict):
            # Flatten nested dict: cluster.name -> CLUSTER_NAME
            for sub_key, sub_val in val.items():
                var_name = f"{var_prefix}_{sub_key.upper()}"
                _emit_value(var_name, sub_val)

        elif isinstance(val, list):
            if len(val) == 0:
                print(f"{var_prefix}=()")
                print(f"{var_prefix}_COUNT=0")
            elif all(isinstance(item, dict) for item in val):
                # List of dicts (e.g. agents)
                print(f"{var_prefix}_COUNT={len(val)}")
                for i, item in enumerate(val):
                    for k, v in item.items():
                        var_name = f"{var_prefix}_{i}_{k.upper()}"
                        _emit_value(var_name, v)
            else:
                # List of scalars
                items = ' '.join(sh_quote(x) for x in val)
                print(f"{var_prefix}=({items})")
                print(f"{var_prefix}_COUNT={len(val)}")
        else:
            _emit_value(var_prefix, val)


def _emit_value(var_name, val):
    """Emit a single bash variable assignment."""
    if isinstance(val, list):
        if len(val) == 0:
            print(f"{var_name}=()")
            print(f"{var_name}_COUNT=0")
        else:
            items = ' '.join(sh_quote(x) for x in val)
            print(f"{var_name}=({items})")
            print(f"{var_name}_COUNT={len(val)}")
    elif isinstance(val, dict):
        for k, v in val.items():
            _emit_value(f"{var_name}_{k.upper()}", v)
    else:
        print(f"{var_name}={sh_quote(val)}")


if __name__ == '__main__':
    data = parse_yaml(sys.argv[1])
    emit_bash_vars(data)
PY_YAML
)

# ---- Public API ----

load_cluster_config() {
    local config_file="${1:-${SCRIPT_DIR}/cluster-config.yaml}"

    if [[ ! -f "$config_file" ]]; then
        echo -e "\033[0;31m[fatal]\033[0m Config file not found: $config_file" >&2
        return 1
    fi

    if ! command -v python3 >/dev/null 2>&1; then
        echo -e "\033[0;31m[fatal]\033[0m python3 is required but not installed" >&2
        return 1
    fi

    local parsed
    parsed="$(python3 -c "$PY_YAML_PARSER" "$config_file")" || {
        echo -e "\033[0;31m[fatal]\033[0m Failed to parse YAML config: $config_file" >&2
        return 1
    }

    eval "$parsed"
}
