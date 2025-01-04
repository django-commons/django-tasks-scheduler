<!-- omit in toc -->


Django Tasks Scheduler
===================
[![Django CI](https://github.com/django-commons/django-tasks-scheduler/actions/workflows/test.yml/badge.svg)](https://github.com/django-commons/django-tasks-scheduler/actions/workflows/test.yml)
![badge](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/cunla/b756396efb895f0e34558c980f1ca0c7/raw/django-tasks-scheduler-4.json)
[![badge](https://img.shields.io/pypi/dm/django-tasks-scheduler)](https://pypi.org/project/django-tasks-scheduler/)

Documentation can be found in https://django-tasks-scheduler.readthedocs.io/en/latest/

# Sponsor

django-tasks-scheduler is developed for free.

You can support this project by becoming a sponsor using [this link](https://github.com/sponsors/cunla).





# Contributing to django-tasks-scheduler

First off, thanks for taking the time to contribute! ❤️

All types of contributions are encouraged and valued. See the [Table of Contents](#table-of-contents) for different ways
to help and details about how this project handles them. Please make sure to read the relevant section before making
your contribution. It will make it a lot easier for us maintainers and smooth out the experience for all involved. The
community looks forward to your contributions. 🎉

> And if you like the project, but just don't have time to contribute, that's fine. There are other easy ways to support
> the project and show your appreciation, which we would also be very happy about:
> - Star the project
> - Tweet about it
> - Refer to this project in your project's readme
> - Mention the project at local meetups and tell your friends/colleagues

<!-- omit in toc -->

## Table of Contents

- [Django Tasks Scheduler](#django-tasks-scheduler)
- [Sponsor](#sponsor)
- [Contributing to django-tasks-scheduler](#contributing-to-django-tasks-scheduler)
  - [Table of Contents](#table-of-contents)
  - [Code of Conduct](#code-of-conduct)
  - [I Have a Question](#i-have-a-question)
  - [I Want To Contribute](#i-want-to-contribute)
    - [Reporting Bugs](#reporting-bugs)
      - [Before Submitting a Bug Report](#before-submitting-a-bug-report)
      - [How Do I Submit a Good Bug Report?](#how-do-i-submit-a-good-bug-report)
    - [Suggesting Enhancements](#suggesting-enhancements)
      - [Before Submitting an Enhancement](#before-submitting-an-enhancement)
      - [How Do I Submit a Good Enhancement Suggestion?](#how-do-i-submit-a-good-enhancement-suggestion)
    - [Your First Code Contribution](#your-first-code-contribution)
    - [Improving The Documentation](#improving-the-documentation)
  - [Style guides](#style-guides)
    - [Commit Messages](#commit-messages)
  - [Join The Project Team](#join-the-project-team)
  - [Attribution](#attribution)

## Code of Conduct

This project and everyone participating in it is governed by the
[django-tasks-scheduler Code of Conduct](https://github.com/django-commons/django-tasks-scheduler/blob/main/CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code. Please report unacceptable behavior
to <daniel.maruani@gmail.com>.

## I Have a Question

> If you want to ask a question, we assume that you have read the
> available [Documentation](https://github.com/django-commons/django-tasks-scheduler).

Before you ask a question, it is best to search for
existing [Issues](https://github.com/django-commons/django-tasks-scheduler/issues) that might help you. In case you have
found a suitable issue and still need clarification, you can write your question in this issue. It is also advisable to
search the internet for answers first.

If you then still feel the need to ask a question and need clarification, we recommend the following:

- Open an [Issue](https://github.com/django-commons/django-tasks-scheduler/issues/new).
- Provide as much context as you can about what you're running into.
- Provide project and platform versions (nodejs, npm, etc), depending on what seems relevant.

We will then take care of the issue as soon as possible.

<!--
You might want to create a separate issue tag for questions and include it in this description. People should then tag their issues accordingly.

Depending on how large the project is, you may want to outsource the questioning, e.g., to Stack Overflow or Gitter. You may add additional contact and information possibilities:
- IRC
- Slack
- Gitter
- Stack Overflow tag
- Blog
- FAQ
- Roadmap
- E-Mail List
- Forum
-->

## I Want To Contribute

> ### Legal Notice <!-- omit in toc -->
> When contributing to this project, you must agree that you have authored 100% of the content, that you have the
> necessary rights to the content and that the content you contribute may be provided under the project license.

### Reporting Bugs

<!-- omit in toc -->

#### Before Submitting a Bug Report

A good bug report shouldn't leave others needing to chase you up for more information. Therefore, we ask you to
investigate carefully, collect information and describe the issue in detail in your report. Please complete the
following steps in advance to help us fix any potential bug as fast as possible.

- Make sure that you are using the latest version.
- Determine if your bug is really a bug and not an error on your side, e.g., using incompatible environment
  components/versions (Make sure that you have read
  the [documentation](https://github.com/django-commons/django-tasks-scheduler). If you are looking for support, you might
  want to check [this section](#i-have-a-question)).
- To see if other users have experienced (and potentially already solved) the same issue you are having, check if there
  is not already a bug report existing for your bug or error in
  the [bug tracker](https://github.com/django-commons/django-tasks-scheduler/issues?q=label%3Abug).
- Also make sure to search the internet (including Stack Overflow) to see if users outside the GitHub community have
  discussed the issue.
- Collect information about the bug:
    - Stack trace (Traceback)
    - OS, Platform and Version (Windows, Linux, macOS, x86, ARM)
    - Version of the interpreter, compiler, SDK, runtime environment, package manager, depending on what seems relevant.
    - Possibly your input and the output
    - Can you reliably reproduce the issue? And can you also reproduce it with older versions?

<!-- omit in toc -->

#### How Do I Submit a Good Bug Report?

> You must never report security related issues, vulnerabilities or bugs, including sensitive information to the issue
> tracker, or elsewhere in public. Instead, sensitive bugs must be sent by email to <daniel.maruani@gmail.com>.
<!-- You may add a PGP key to allow the messages to be sent encrypted as well. -->

We use GitHub issues to track bugs and errors. If you run into an issue with the project:

- Open an [Issue](https://github.com/django-commons/django-tasks-scheduler/issues/new).
  (Since we can't be sure at this point whether it is a bug or not, we ask you not to talk about a bug yet and 
  not to label the issue.)
- Explain the behavior you would expect and the actual behavior.
- Please provide as much context as possible and describe the *reproduction steps* that someone else can follow to
  recreate the issue on their own. This usually includes your code.  
  For good bug reports, you should isolate the problem and create a reduced test case.
- Provide the information you collected in the previous section.

Once it's filed:

- The project team will label the issue accordingly.
- A team member will try to reproduce the issue with your provided steps. If there are no reproduction steps or no
  obvious way to reproduce the issue, the team will ask you for those steps and mark the issue as `needs-repro`. Bugs
  with the `needs-repro` tag will not be addressed until they are reproduced.
- If the team is able to reproduce the issue, it will be marked `needs-fix`, as well as possibly other tags (such
  as `critical`), and the issue will be left to be [implemented by someone](#your-first-code-contribution).

<!-- You might want to create an issue template for bugs and errors that can be used as a guide and that defines the structure of the information to be included. If you do so, reference it here in the description. -->

### Suggesting Enhancements

This section guides you through submitting an enhancement suggestion for django-tasks-scheduler, **including completely
new
features and minor improvements to existing functionality**. Following these guidelines will help maintainers and the
community to understand your suggestion and find related suggestions.

<!-- omit in toc -->

#### Before Submitting an Enhancement

- Make sure that you are using the latest version.
- Read the [documentation](https://github.com/django-commons/django-tasks-scheduler) carefully and find out if the
  functionality is already covered, maybe by an individual configuration.
- Perform a [search](https://github.com/django-commons/django-tasks-scheduler/issues) to see if the enhancement has
  already
  been suggested. If it has, add a comment to the existing issue instead of opening a new one.
- Find out whether your idea fits with the scope and aims of the project. It's up to you to make a strong case to
  convince the project's developers of the merits of this feature. Keep in mind that we want features that will be
  useful to the majority of our users and not just a small subset. If you're just targeting a minority of users,
  consider writing an add-on/plugin library.

<!-- omit in toc -->

#### How Do I Submit a Good Enhancement Suggestion?

Enhancement suggestions are tracked as [GitHub issues](https://github.com/django-commons/django-tasks-scheduler/issues).

- Use a **clear and descriptive title** for the issue to identify the suggestion.
- Provide a **step-by-step description of the suggested enhancement** in as many details as possible.
- **Describe the current behavior** and **explain which behavior you expected to see instead** and why. At this point
  you can also tell which alternatives do not work for you.
- You may want to **include screenshots and animated GIFs** which help you demonstrate the steps or point out the part
  which the suggestion is related to. You can use [this tool](https://www.cockos.com/licecap/) to record GIFs on macOS
  and Windows, and [this tool](https://github.com/colinkeenan/silentcast)
  or [this tool](https://github.com/GNOME/byzanz) on
  Linux. <!-- this should only be included if the project has a GUI -->
- **Explain why this enhancement would be useful** to most django-tasks-scheduler users. You may also want to point out
  the
  other projects that solved it better and which could serve as inspiration.

<!-- You might want to create an issue template for enhancement suggestions that can be used as a guide and that defines the structure of the information to be included. If you do so, reference it here in the description. -->

### Your First Code Contribution

Unsure where to begin contributing? You can start by looking through
[help-wanted issues](https://github.com/django-commons/django-tasks-scheduler/labels/help%20wanted).

Never contributed to open source before? Here are a couple of friendly
tutorials:

- <http://makeapullrequest.com/>
- <http://www.firsttimersonly.com/>

### Improving The Documentation

- Create your own fork of the repository
- Do the changes in your fork.
- Create a pull request with the changes.

## Style guides

### Commit Messages

Taken from [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/)

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

The commit message contains the following structural elements,
in order to communicate intent to the consumers of your library:

* `fix:` a commit of the type fix patches a bug in your codebase (this correlates with `PATCH` in Semantic Versioning).
* `feat:` a commit of the type feat introduces a new feature to the codebase (this correlates with `MINOR` in Semantic
  Versioning).
* `BREAKING CHANGE:` a commit that has a footer BREAKING CHANGE:, or appends a ! after the type/scope, introduces a
  breaking API change (correlating with MAJOR in Semantic Versioning). A BREAKING CHANGE can be part of commits of any
  type.
* types other than `fix:` and `feat:` are allowed, for example, @commitlint/config-conventional (based on the Angular
  convention) recommends `build:`, `chore:`, `ci:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`, and others.
* footers other than `BREAKING CHANGE: <description>` may be provided and follow a convention similar to
  [git trailer format](https://git-scm.com/docs/git-interpret-trailers).

Additional types are not mandated by the Conventional Commits specification, and have no implicit effect in Semantic
Versioning (unless they include a BREAKING CHANGE). A scope may be provided to a commit’s type, to provide additional
contextual information and is contained within parenthesis, e.g., feat(parser): add ability to parse arrays.

## Join The Project Team

If you wish to be added to the project team as a collaborator, please create an issue 
explaining why you wish to join the team and tag @cunla in the issue.

<!-- omit in toc -->

## Attribution

This guide is based on the **contributing-gen**. [Make your own](https://github.com/bttger/contributing-gen)!
