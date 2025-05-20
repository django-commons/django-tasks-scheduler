EXTRA_ARGS=""
x =  ("json","bf","lua","cf")
for extra in x[@]; do
    EXTRA_ARGS+="--extra $extra"
  done
echo "Command: uv sync $EXTRA_ARGS"
