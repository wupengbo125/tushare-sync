sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json <<EOF
{
    "registry-mirrors": [
        "https://docker.1ms.run"
    ]
}
EOF
sudo systemctl daemon-reload
sudo systemctl restart docker

bash <(curl -f -s --connect-timeout 10 --retry 3 https://linuxmirrors.cn/docker.sh) --source mirrors.tencent.com/docker-ce --source-registry docker.1ms.run --protocol https --install-latested true --close-firewall false --ignore-backup-tips

docker pull docker.1ms.run/library/mysql:8.0
