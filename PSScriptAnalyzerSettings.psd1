@{
  IncludeRules = @(
    'PSUseConsistentIndentation','PSUseConsistentWhitespace',
    'PSAvoidUsingCmdletAliases','PSAvoidUsingPositionalParameters'
  )
  Rules = @{
    PSUseConsistentIndentation = @{ Kind='space'; IndentationSize=2; Enable=$true }
    PSUseConsistentWhitespace  = @{ CheckPipe=$true }
  }
  Severity = @{
    PSUseConsistentIndentation='Information';
    PSUseConsistentWhitespace='Information'
  }
}
