import pywencai

import pandas as pd


def get_concept_index():

    print("正在获取同花顺概念指数列表...")

    return pywencai.get(query="同花顺概念指数", query_type="zhishu", sort_order='desc', loop=True)

def main():

# 获取概念指数数据

    print("开始获取同花顺概念板块数据...")

    df = get_concept_index()

    # 显示获取到的概念板块数量

    print(f"共获取到 {len(df)} 个概念板块")

# 设置保存路径

    save_path = r"./a.csv"

# 保存为CSV文件

    df.to_csv(save_path, index=False, encoding='utf-8-sig')

if __name__ == "__main__":

    main()
