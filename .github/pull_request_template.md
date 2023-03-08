
# Description

*Please add a description here. This will become the commit message of the merge request later.*

<!-- Commit message above. Everything below is not added to the message. Do not change this line! -->

## Definition of Done Checklist

- Not all of these items are applicable to all PRs, the author should update this template to only leave the boxes in that are relevant
- Please make sure all these things are done and tick the boxes
 
```[tasklist]
# Author
- [ ] Changes are OpenShift compatible
- [ ] CRD changes approved
- [ ] Helm chart can be installed and deployed operator works
- [ ] Integration tests passed (for non trivial changes)
```

```[tasklist]
# Reviewer
- [ ] Code contains useful comments
- [ ] (Integration-)Test cases added
- [ ] Documentation added or updated
- [ ] Changelog updated
- [ ] Cargo.toml only contains references to git tags (not specific commits or branches)
```

```[tasklist]
# Acceptance
- [ ] Feature Tracker has been updated
- [ ] Proper release label has been added
```

Once the review is done, comment `bors r+` (or `bors merge`) to merge. [Further information](https://bors.tech/documentation/getting-started/#reviewing-pull-requests)
