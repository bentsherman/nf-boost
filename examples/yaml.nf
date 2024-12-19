
include { fromYaml ; toYaml } from 'plugin/nf-boost'

workflow {
    value = [
        [id: '1', files: []],
        [id: '2', files: []],
        [id: '3', files: []],
    ]
    yaml = toYaml(value)
    println yaml
    assert fromYaml(yaml) == value
}
