#!/usr/bin/env bash
# =============================================================================
#  Go 项目 pre-commit 核心逻辑库（完全模块化、可被其他项目 import）
#  使用方式：source 本文件后调用 pre_commit::run
#  git config core.hooksPath .hook
# =============================================================================

pre_commit::log() {
    printf '%b\n' "$*" >&2
    sync >&2 || true  # 强制刷新
}

pre_commit::info() {
    printf '\033[36mℹ\033[0m %s\n' "$*" >&2
    sync >&2 || true
}

pre_commit::success() {
    printf '\033[32m✓\033[0m %s\n' "$*" >&2
    sync >&2 || true
}

pre_commit::fail() {
    printf '\033[31m✗\033[0m %s\n' "$*" >&2
    sync >&2 || true
    exit 1
}

# 简单的 spinner 动画，用于命令执行时的实时反馈
pre_commit::spinner() {
    local msg=$1 pid=$2
    local i=0
    #local spin='◴◷◶◵'    # 这四个字符在 Terminal.app 里居然奇迹般地正常！（唯一例外）

    # 如果上面四个还是有问题，就用最保险的：
    # local spin='⣾⣽⣻⢿⡿⣟⣯⣷'
    # 最美的
    local spin='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏'

    while kill -0 "$pid" 2>/dev/null; do
        printf '\r\033[36m%s\033[0m %s\033[K' "${spin:i++:1}" "$msg" >&2
        (( i == ${#spin} )) && i=0
        sleep 0.1
    done

    wait "$pid" 2>/dev/null; ret=$?
    # 判断 $msg 是否为空，避免多余的空行
    [[ -z "$msg" ]] && return $ret
    
    if (( ret == 0 )); then
        # 可替代符号 ✓s
        printf '\r\033[32m✓\033[0m %s \033[32m成功\033[0m\n' "$msg" >&2
        # 在信息行末显示完成状态
        # printf '\r%-35s \033[32m完成\033[0m\n' "$msg" >&2
    else
        # 可替代符号 ✗
        printf '\r\033[31m✗\033[0m %s (失败)\n' "$msg" >&2

        # 在信息行末显示失败状态
        # printf '\r%-35s \033[31m失败\033[0m\n' "$msg" >&2
        # pre_commit::fail "$msg 失败"
    fi
    sync >&2 || true # 强制刷新
    return "$ret"
}

pre_commit::usage() {
    cat >&2 <<'EOF'

用法: pre-commit [选项]

选项:
    --skip-tests               跳过单元测试
    --skip-lint                跳过 golangci-lint
    --only-dirs   dir1,dir2    只处理指定目录（逗号分隔）
    --skip-dirs   dir1,dir2    跳过指定目录（逗号分隔）
    --verbose                  开启详细日志
    --help                     显示帮助

示例:
    # 手动执行，跳过单元测试
    pre-commit --skip-tests
    # 手动执行，指定只处理某些目录
    pre-commit --only-dirs internal/api,internal/svc

    # git rebase/amend 时跳过测试和某些目录
    SKIP_PRECOMMIT_TESTS=1 PRECOMMIT_SKIP_DIRS=etc,mock git commit --amend

    # git rebase/amend 时输出详细日志
    PRECOMMIT_VERBOSE=1 git commit --amend

环境变量:
    SKIP_PRECOMMIT_TESTS       同 --skip-tests
    SKIP_PRECOMMIT_LINT        同 --skip-lint
    PRECOMMIT_ONLY_DIRS        同 --only-dirs
    PRECOMMIT_SKIP_DIRS        同 --skip-dirs
    PRECOMMIT_VERBOSE          同 --verbose
EOF
    exit 1
}

pre_commit::parse_args() {
    # 1. 环境变量优先级最高（解决 rebase/merge/amend 场景）
    SKIP_TESTS=${SKIP_PRECOMMIT_TESTS:-0}
    SKIP_LINT=${SKIP_PRECOMMIT_LINT:-0}

    # 通过 SKIP_PRECOMMIT_TESTS=1 或 SKIP_PRECOMMIT_LINT=1 跳过测试或 Lint
    # 通过 PRECOMMIT_SKIP_DIRS=etc,internal/svc,mock 跳过指定目录
    ONLY_DIRS=()
    SKIP_DIRS=()
    if [[ -n "${PRECOMMIT_ONLY_DIRS:-}" ]]; then
        IFS=',' read -ra ONLY_DIRS <<< "$PRECOMMIT_ONLY_DIRS"
    fi
    if [[ -n "${PRECOMMIT_SKIP_DIRS:-}" ]]; then
        IFS=',' read -ra SKIP_DIRS <<< "$PRECOMMIT_SKIP_DIRS"
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --skip-tests)   SKIP_TESTS=1; shift ;;
            --skip-lint)    SKIP_LINT=1; shift ;;
            --only-dirs)    IFS=',' read -ra ONLY_DIRS <<< "$2"; shift 2 ;;
            --skip-dirs)    IFS=',' read -ra SKIP_DIRS <<< "$2"; shift 2 ;;
            --verbose)      set -x; shift ;;
            --help|-h)      pre_commit::usage ;;
            *)              pre_commit::log "未知参数: $1"; pre_commit::usage ;;
        esac
    done

    # pre_commit::info "配置：跳过测试=${SKIP_TESTS}, 跳过 Lint=${SKIP_LINT}"
    # pre_commit::info "配置：只处理目录=(${ONLY_DIRS[*]:-无}), 跳过目录=(${SKIP_DIRS[*]:-无})"
    # 支持环境变量一键开启详细模式（rebase 时超好用）
    # [[ -n "${PRECOMMIT_VERBOSE:-}" ]] && set -x
}

# ====================== 工具检查 ======================
# 检查所需工具是否安装
pre_commit::check_tools() {
    local missing=()
    for cmd in go gofumpt gci golangci-lint make; do
        command -v "$cmd" >/dev/null || missing+=("$cmd")
    done
    (( ${#missing[@]} > 0 )) && pre_commit::fail "缺少工具：${missing[*]}\n   请执行：brew install go gofumpt gci golangci-lint make"
    pre_commit::success "工具检测通过"
}

# ====================== 目录过滤器 ======================
pre_commit::should_process_dir() {
    local dir=$1
    [[ -z "$dir" || "$dir" == "." ]] && return 1

    # --only-dirs 优先级最高
    if (( ${#ONLY_DIRS[@]} > 0 )); then
        for d in "${ONLY_DIRS[@]}"; do [[ "$dir" == *"$d"* ]] && return 0; done
        return 1
    fi

    # --skip-dirs
    for d in "${SKIP_DIRS[@]}"; do
        d=$(echo "$d" | xargs)  # 去除前后空格
        if [[ "$dir" == *"$d"* ]]; then
            return 1
        fi
    done

    return 0
}

# 增量构建
#
# @param staged 变更的文件列表，通过参数传入
# @return 无
pre_commit::incremental_build() {
    # 获取参数
    local -a staged=("$@")
    local -a targets=()
    for f in "${staged[@]}"; do
        case "$f" in
            consumer/exhibition-sync/*)      targets+=(exhibit-sync) ;;
            consumer/gemdrop-indexer/*)      targets+=(gemdrop-indexer) ;;
            consumer/gemdrop-cleaning/*)     targets+=(gemdrop-cleaning) ;;
            # 其他变更不触发构建
            *) ;;
        esac
    done

    # 为空则退出
    (( ${#targets[@]} == 0 )) && return

    # 去重（如果为空会导致 targets 中加入一个空元素）
    mapfile -t targets < <(printf '%s\n' "${targets[@]}" | sort -u)
    if (( ${#targets[@]} > 0 )); then
        (
            for t in "${targets[@]}"; do
                local TMP_LOG
                TMP_LOG=$(mktemp) || exit 1

                pre_commit::log " $t \033[32m✓\033[0m"
                if ! make -j "$t" >> "$TMP_LOG" 2>&1; then
                    pre_commit::log "\n$(cat "$TMP_LOG")"
                    pre_commit::fail "构建 $t 失败，请修复后重新提交"
                fi
                rm -f "$TMP_LOG"
            done
            exit 0                          # 成功
        ) & pre_commit::spinner "构建" $! # $! 表示后台任务的 PID，用于 spinner 跟踪
    fi
}

# 处理 go 文件：gofumpt、gci、golangci-lint
#
# @param staged 变更的文件列表，通过参数传入
# @return 无
pre_commit::process_go_files() {
    local -a staged=("$@")
    local -a go_files=()
    for f in "${staged[@]}"; do [[ "$f" == *.go ]] && go_files+=("$f"); done
    if (( ${#go_files[@]} > 0 )); then
        (
            for f in "${go_files[@]}"; do
                pre_commit::log " $f \033[32m✓\033[0m"

                gofumpt -w "$f"
                gci write -s standard -s default -s "prefix(bitbucket.kucoin.net)" -s localmodule --skip-generated "$f"
            done
            printf '%s\0' "${go_files[@]}" | xargs -0 git add >/dev/null 2>&1
        ) & pre_commit::spinner "格式化" $!
    fi

    # Lint 检查
    if (( SKIP_LINT == 0 && ${#go_files[@]} > 0 )); then
        mapfile -t dirs < <(printf '%s\n' "${go_files[@]}" | xargs dirname | sort -u)
        local -a lint_dirs=()
        for lintd in "${dirs[@]}"; do
            if pre_commit::should_process_dir "$lintd"; then
                lint_dirs+=("$lintd")
            fi
        done

        # 去重
        mapfile -t lint_dirs < <(printf '%s\n' "${lint_dirs[@]}" | sort -u)

        if (( ${#lint_dirs[@]} > 0 )); then
            (
                for d in "${lint_dirs[@]}"; do
                    # 跳过目录名为空的情况
                    [[ -z "$d" ]] && continue

                    local TMP_LOG
                    TMP_LOG=$(mktemp) || exit 1

                    pre_commit::log " $d/... \033[32m✓\033[0m"
                    if ! golangci-lint run --config .golangci.yml --fix "$d" >> "$TMP_LOG" 2>&1; then
                        pre_commit::log "\n$(cat "$TMP_LOG")"
                        pre_commit::fail "Lint 检查失败，请修复后重新提交"
                    fi
                    rm -f "$TMP_LOG"
                done
                exit 0                          # 成功
            ) & pre_commit::spinner "Lint" $! # $! 表示后台任务的 PID，用于 spinner 跟踪
            for f in "${go_files[@]}"; do git diff --quiet "$f" 2>/dev/null || git add "$f" >/dev/null; done
        else
            pre_commit::success "所有变更目录均被跳过，跳过 Lint"
        fi
    fi
}

# 运行 go test
pre_commit::run_tests() {
    local -a staged=("$@")

    if (( SKIP_TESTS == 0 )); then
        local -a test_dirs=()
        for f in "${staged[@]}"; do
            [[ "$f" == *_test.go ]] && test_dirs+=("$(dirname "$f")")
        done
        mapfile -t test_dirs < <(printf '%s\n' "${test_dirs[@]}" | sort -u)
        local -a final_test_dirs=()
        for d in "${test_dirs[@]}"; do
            pre_commit::should_process_dir "$d" && final_test_dirs+=("$d")
        done

        if (( ${#final_test_dirs[@]} == 0 )); then
            pre_commit::success "无测试目录需要运行"
            return
        fi

        if (( ${#final_test_dirs[@]} > 0 )); then
            (
                for d in "${final_test_dirs[@]}"; do
                    [[ -z "$d" ]] && continue

                    local TMP_LOG
                    TMP_LOG=$(mktemp) || exit 1
                    pre_commit::log " $d \033[32m✓\033[0m"

                    # 输出到临时日志文件，避免干扰 spinner，错误时输出日志内容
                    if ! go test -gcflags=all=-l -race -v -short -failfast -parallel=1 -count=1 "./$d/..." >> "$TMP_LOG" 2>&1; then
                        pre_commit::log "\n$(cat "$TMP_LOG")"
                        pre_commit::fail "单元测试失败，请修复后重新提交"
                    fi
                    rm -f "$TMP_LOG"
                done

                exit 0                          # 成功
            ) & pre_commit::spinner "测试" $!
        fi
    fi
}

pre_commit::run() {
    pre_commit::parse_args "$@"
    pre_commit::log "\n\033[1;36m🚀 开始 pre-commit 检查\033[0m\n"

    local repo_root
    repo_root=$(git rev-parse --show-toplevel)
    cd "$repo_root" || exit

    mapfile -t staged < <(git diff --cached --name-only --diff-filter=ACMR)
    (( ${#staged[@]} == 0 )) && { pre_commit::success "无文件变更，跳过检查"; return 0; }

    pre_commit::check_tools

    # 增量构建（没有需要构建的）
    # pre_commit::incremental_build "${staged[@]}"

    # 处理 Go 文件
    pre_commit::process_go_files "${staged[@]}"

    # 单元测试
    pre_commit::run_tests "${staged[@]}"
    pre_commit::log "\n\033[1;32m🎉 所有检查通过，提交成功！\033[0m\n"
}
