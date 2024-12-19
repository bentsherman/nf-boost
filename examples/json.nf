
include { fromJson ; toJson } from 'plugin/nf-boost'

workflow {
    value = [
        [id: '1', files: []],
        [id: '2', files: []],
        [id: '3', files: []],
    ]
    json = toJson(value, true)
    println json
    assert fromJson(json) == value
}
