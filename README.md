# IV Analyzer — 部署说明

## 文件结构

```
iv-analyzer/
├── index.html                    ← 主页面
├── netlify.toml                  ← Netlify 配置
├── netlify/
│   └── functions/
│       └── store.mjs             ← 分享链接存储函数
└── README.md
```

## 部署步骤（Netlify — 免费）

### 方法一：GitHub + Netlify（推荐，可自动更新）

1. 注册/登录 [GitHub](https://github.com)，新建一个仓库（可设为 Private）
2. 把这个文件夹里的所有文件上传到仓库
3. 登录 [Netlify](https://app.netlify.com)，点 **"Add new site" → "Import an existing project"**
4. 选择刚才的 GitHub 仓库，Build settings 保持默认，点 **Deploy**
5. 部署完成后访问 Netlify 给的域名即可

### 方法二：Netlify CLI（本地一键部署）

```bash
npm install -g netlify-cli
netlify login
cd iv-analyzer
netlify deploy --prod
```

---

## 注意

- 分享链接功能需要部署到 Netlify 后才能使用（本地直接打开 HTML 无法使用分享功能）
- 数据存储在 Netlify Blobs（免费，无需额外配置）
- 每次上传新数据后需要重新点"生成分享链接"
