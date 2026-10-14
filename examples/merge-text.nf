
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
  ch_records = channel.of( 1..10 ).map { i -> makeRecord(i) }

  ch_csv = RECORD_TO_CSV(ch_records)

  ch_items = ch_csv.map { _meta, csv -> csv }.collect()

  ch_txt = ITEMS_TO_TXT(ch_items)
  ch_txt.view { txt -> txt.text }
}


workflow GROUP_SORT_MERGE_TEXT {
  ch_records = channel.of( 1..10 ).map { i -> makeRecord(i) }

  ch_csv = RECORD_TO_CSV(ch_records)

  ch_groups = ch_csv
    .map { meta, csv -> [meta.type, [meta, csv]] }
    .groupTuple()
    .map { group, items ->
      def sorted = items
        .sort { item -> item[0].id }
        .collect { _meta, csv -> csv }
      return tuple(group, sorted)
    }

  ch_txt = GROUPS_TO_TXT(ch_groups)
  ch_txt.view { txt -> txt.text }
}


workflow {
  MERGE_TEXT()
}
