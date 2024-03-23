
include { mergeCsv ; mergeText } from 'plugin/nf-boost'


process RECORD_TO_CSV {
  input:
  val record

  output:
  tuple val(record.id), path("record.${record.id}.txt")

  exec:
  def path = task.workDir.resolve("record.${record.id}.txt")
  mergeCsv([ record ], path, header: true, sep: '\t')
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


workflow {
  Channel.of( 1..10 )
    | map { i -> String.format('%02d', i) }
    | map { id -> ['id': id, 'name': "record_${id}"] }
    | RECORD_TO_CSV
    | toSortedList { a, b -> a[0] <=> b[0] }
    | map { items -> items.collect((id, csv) -> csv) }
    | ITEMS_TO_TXT
    | view { it.text }
}
