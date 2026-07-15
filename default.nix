{ pkgs ? import <nixpkgs> {} }:

  let
    importedPkgs = import (builtins.fetchTarball {
        url = "https://github.com/NixOS/nixpkgs/archive/1b7a6a6e57661d7d4e0775658930059b77ce94a4.tar.gz";

    }) {};

    myMKL = importedPkgs.mkl;

in (pkgs.buildFHSUserEnv {
  name = "pipzone";
  targetPkgs = pkgs: (with pkgs; [
    gcc13
    graphviz
    taglib
    openssl
    git
    git-lfs
    libxml2
    libxslt
    libzip
    zlib
    python310
    x11_ssh_askpass
    myMKL
  ]);
}).env
