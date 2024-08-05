
include { mergeCsv ; mergeText } from 'plugin/nf-boost'


process RECORD_TO_CSV {
  input:
  val record

  output:
  tuple val(meta), path("record.${record.id}.txt")

  exec:
  def path = task.workDir.resolve("record.${record.id}.txt")
  mergeCsv([ record ], path, header: true, sep: '\t')
  meta = [id: record.id, type: record.type]
}


process ITEMS_TO_TXT {
  publishDir 'results'

  input:
  val items

  output:
  path 'items.txt'

  exec:
  def path = task.workDir.resolve('items.txt')
  mergeText(items, path, keepHeader: true)
}


process GROUPS_TO_TXT {
  publishDir 'results'

  input:
  tuple val(group), val(items)

  output:
  path "items.${group}.txt"

  exec:
  def path = task.workDir.resolve("items.${group}.txt")
  mergeText(items, path, keepHeader: true)
}


def makeRecord(int i) {
  def id = String.format('%02d', i)
  return [
    id: id,
    type: i % 2 == 0 ? 'even' : 'odd',
    name: "record_${id}"
  ]
}


workflow MERGE_TEXT {
  Channel.of( 1..10 )
    | map { i -> makeRecord(i) }
    | RECORD_TO_CSV
    | map { meta, csv -> csv }
    | collect
    | ITEMS_TO_TXT
    | view { txt -> txt.text }
}


workflow GROUP_SORT_MERGE_TEXT {
  Channel.of( 1..10 )
    | map { i -> makeRecord(i) }
    | RECORD_TO_CSV
    | map { meta, csv -> [meta.type, [meta, csv]] }
    | groupTuple
    | map { group, items ->
      def sorted = items
        .sort { item -> item[0].id }
        .collect { meta, csv -> csv }
      return [group, sorted]
    }
    | GROUPS_TO_TXT
    | view { txt -> txt.text }
}


workflow {
  MERGE_TEXT()
}
