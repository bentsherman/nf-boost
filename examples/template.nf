
include { template } from 'plugin/nf-boost'

workflow {
  binding = [
    firstName: "Grace",
    lastName: "Hopper",
    accepted: true,
    title: 'Groovy for COBOL programmers'
  ]
  text = '''\
    Dear $firstName $lastName,

    We ${ accepted ? 'are pleased' : 'regret' } to inform you that your paper entitled '$title' was ${ accepted ? 'accepted' : 'rejected' }.

    The conference committee.
    '''.stripIndent()

  println template(text, binding)
}
