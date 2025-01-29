cat example_database.json | ETCDCTL_API=3 etcdctl put '/landtable/databases/'(cat example_database.json | jq -r .id)
cat example_workspace.json | ETCDCTL_API=3 etcdctl put '/landtable/workspaces/'(cat example_workspace.json | jq -r .id)'/meta'
cat example_meta.json | ETCDCTL_API=3 etcdctl put '/landtable/meta'
ETCDCTL_API=3 etcdctl put '/landtable/workspaceAliases/highseas' (cat example_workspace.json | jq -r .id)