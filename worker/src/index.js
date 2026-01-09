// 主Worker文件 - 简化版（无Queue依赖）
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    
    // 健康检查
    if (url.pathname === '/' || url.pathname === '/health') {
      return jsonResponse({
        service: 'VOD API Service (Free Version)',
        version: '1.0.0',
        status: 'healthy',
        endpoints: {
          api: '/api/vod/*',
          admin: '/admin/collect?api_key=YOUR_KEY',
          stats: '/api/vod/stats'
        },
        note: 'No Queue dependency, using direct Cron Trigger'
      });
    }
    
    // API路由
    if (url.pathname.startsWith('/api/vod')) {
      return handleAPI(request, env, ctx);
    }
    
    // 管理端点 - 手动触发采集
    if (url.pathname === '/admin/collect') {
      return handleCollect(request, env, ctx);
    }
    
    // Cron触发端点
    if (url.pathname === '/cron/collect') {
      return handleCron(request, env, ctx);
    }
    
    return jsonResponse({ error: 'Not Found' }, 404);
  }
};

// API处理器
async function handleAPI(request, env, ctx) {
  const url = new URL(request.url);
  const path = url.pathname;
  
  try {
    // 获取视频列表
    if (path === '/api/vod/list') {
      const page = parseInt(url.searchParams.get('page')) || 1;
      const limit = Math.min(100, parseInt(url.searchParams.get('limit')) || 20);
      const offset = (page - 1) * limit;
      const typeId = url.searchParams.get('type_id');
      
      const db = env.DB;
      
      // 构建查询
      let whereClause = '';
      let params = [limit, offset];
      
      if (typeId) {
        whereClause = 'WHERE type_id = ?';
        params = [typeId, limit, offset];
      }
      
      // 获取总数
      const countQuery = typeId 
        ? 'SELECT COUNT(*) as total FROM vod_list WHERE type_id = ?'
        : 'SELECT COUNT(*) as total FROM vod_list';
      
      const countParams = typeId ? [typeId] : [];
      const countResult = await db.prepare(countQuery).bind(...countParams).first();
      const total = countResult?.total || 0;
      
      // 获取数据
      const query = `
        SELECT * FROM vod_list 
        ${whereClause}
        ORDER BY vod_time DESC, vod_id DESC 
        LIMIT ? OFFSET ?
      `;
      
      const { results } = await db.prepare(query).bind(...params).all();
      
      // 获取分类
      const { results: classResults } = await db.prepare(
        'SELECT * FROM vod_class ORDER BY type_pid, type_id'
      ).all();
      
      return jsonResponse({
        success: true,
        data: {
          list: results || [],
          class: classResults || [],
          pagination: {
            page,
            limit,
            total,
            totalPages: Math.ceil(total / limit),
            hasNext: page < Math.ceil(total / limit),
            hasPrev: page > 1
          }
        }
      });
    }
    
    // 获取视频详情
    if (path.startsWith('/api/vod/detail/')) {
      const id = path.split('/').pop();
      const db = env.DB;
      
      const result = await db.prepare(
        'SELECT * FROM vod_list WHERE vod_id = ?'
      ).bind(id).first();
      
      if (!result) {
        return jsonResponse({ success: false, error: 'Video not found' }, 404);
      }
      
      return jsonResponse({ success: true, data: result });
    }
    
    // 搜索视频
    if (path === '/api/vod/search') {
      const query = url.searchParams.get('q');
      const limit = parseInt(url.searchParams.get('limit')) || 20;
      
      if (!query || query.length < 2) {
        return jsonResponse({
          success: true,
          data: { list: [], total: 0, query: query || '' }
        });
      }
      
      const db = env.DB;
      const searchQuery = `%${query}%`;
      
      const { results } = await db.prepare(`
        SELECT * FROM vod_list 
        WHERE vod_name LIKE ? OR vod_en LIKE ?
        ORDER BY vod_time DESC 
        LIMIT ?
      `).bind(searchQuery, searchQuery, limit).all();
      
      return jsonResponse({
        success: true,
        data: {
          list: results || [],
          total: results?.length || 0,
          query
        }
      });
    }
    
    // 获取分类
    if (path === '/api/vod/class') {
      const db = env.DB;
      const { results } = await db.prepare(
        'SELECT * FROM vod_class ORDER BY type_pid, type_id'
      ).all();
      
      // 构建树形结构
      const classMap = {};
      results.forEach(item => {
        classMap[item.type_id] = { ...item, children: [] };
      });
      
      const tree = [];
      results.forEach(item => {
        if (item.type_pid === 0) {
          tree.push(classMap[item.type_id]);
        } else if (classMap[item.type_pid]) {
          classMap[item.type_pid].children.push(classMap[item.type_id]);
        }
      });
      
      return jsonResponse({
        success: true,
        data: {
          flat: results,
          tree: tree
        }
      });
    }
    
    // 统计数据
    if (path === '/api/vod/stats') {
      const db = env.DB;
      
      const [
        totalResult,
        recentResult,
        categoryResult
      ] = await Promise.all([
        db.prepare('SELECT COUNT(*) as total FROM vod_list').first(),
        db.prepare("SELECT COUNT(*) as recent FROM vod_list WHERE created_at > datetime('now', '-7 day')").first(),
        db.prepare('SELECT type_name, COUNT(*) as count FROM vod_list GROUP BY type_name ORDER BY count DESC LIMIT 10').all()
      ]);
      
      return jsonResponse({
        success: true,
        data: {
          totalVideos: totalResult?.total || 0,
          recentVideos: recentResult?.recent || 0,
          topCategories: categoryResult?.results || [],
          updatedAt: new Date().toISOString()
        }
      });
    }
    
    return jsonResponse({ success: false, error: 'Endpoint not found' }, 404);
    
  } catch (error) {
    console.error('API error:', error);
    return jsonResponse({ success: false, error: 'Server error' }, 500);
  }
}

// 手动触发采集
async function handleCollect(request, env, ctx) {
  const url = new URL(request.url);
  const apiKey = url.searchParams.get('api_key');
  
  // 验证API密钥
  if (apiKey !== env.ADMIN_API_KEY) {
    return jsonResponse({ error: 'Unauthorized' }, 401);
  }
  
  // 获取采集页数（默认1页）
  const pages = parseInt(url.searchParams.get('pages')) || 1;
  
  // 直接运行采集（同步，会等待采集完成）
  try {
    const result = await collectData(env, pages);
    return jsonResponse({
      message: `Collection completed! Saved ${result.count} videos.`,
      pagesCollected: pages
    });
  } catch (error) {
    return jsonResponse({
      error: 'Collection failed',
      message: error.message
    }, 500);
  }
}

// Cron触发采集
async function handleCron(request, env, ctx) {
  const authHeader = request.headers.get('X-Cron-Auth');
  
  // 简单验证（与wrangler.toml中的cron触发器配合）
  if (authHeader !== 'cron-secret-key') {
    return jsonResponse({ error: 'Unauthorized' }, 401);
  }
  
  // 使用ctx.waitUntil让采集在后台运行
  // 这样即使采集时间较长，也不会阻塞响应
  ctx.waitUntil((async () => {
    try {
      console.log('Cron-triggered collection starting...');
      await collectData(env, 1); // 每天只采集1页最新数据
      console.log('Cron collection completed');
    } catch (error) {
      console.error('Cron collection failed:', error);
    }
  })());
  
  return jsonResponse({ message: 'Cron collection started in background' });
}

// 数据采集函数（核心）
async function collectData(env, maxPages = 1) {
  console.log(`Starting data collection (max pages: ${maxPages})...`);
  
  const db = env.DB;
  const sourceUrl = env.SOURCE_API_URL;
  
  try {
    // 开始事务
    await db.prepare('BEGIN TRANSACTION').run();
    
    let page = 1;
    let totalSaved = 0;
    
    while (page <= maxPages) {
      console.log(`Fetching page ${page}...`);
      
      // 获取数据
      const response = await fetch(`${sourceUrl}?ac=list&pg=${page}`);
      if (!response.ok) {
        throw new Error(`HTTP error: ${response.status}`);
      }
      
      const data = await response.json();
      
      if (data.code !== 1 || !data.list || data.list.length === 0) {
        console.log('No more data available');
        break;
      }
      
      // 处理分类数据（只在第一页）
      if (page === 1 && data.class && data.class.length > 0) {
        console.log(`Processing ${data.class.length} categories...`);
        
        for (const category of data.class) {
          await db.prepare(`
            INSERT OR REPLACE INTO vod_class (type_id, type_pid, type_name, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
          `).bind(
            category.type_id,
            category.type_pid || 0,
            category.type_name
          ).run();
        }
      }
      
      // 处理视频数据
      console.log(`Processing ${data.list.length} videos from page ${page}...`);
      
      for (const video of data.list) {
        try {
          // 转换时间格式
          let vodTime = null;
          if (video.vod_time) {
            try {
              const date = new Date(video.vod_time);
              vodTime = date.toISOString().replace('T', ' ').substring(0, 19);
            } catch (e) {
              // 使用当前时间作为后备
              vodTime = new Date().toISOString().replace('T', ' ').substring(0, 19);
            }
          }
          
          await db.prepare(`
            INSERT OR REPLACE INTO vod_list 
            (vod_id, vod_name, type_id, type_name, vod_en, vod_time, vod_remarks, vod_play_from, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
          `).bind(
            video.vod_id,
            video.vod_name,
            video.type_id,
            video.type_name,
            video.vod_en || '',
            vodTime,
            video.vod_remarks || '',
            video.vod_play_from || ''
          ).run();
          
          totalSaved++;
        } catch (videoError) {
          console.error(`Error saving video ${video.vod_id}:`, videoError.message);
        }
      }
      
      // 检查是否还有更多页
      const totalPages = data.pagecount || 1;
      if (page >= totalPages) {
        console.log(`Reached total pages (${totalPages})`);
        break;
      }
      
      page++;
      
      // 礼貌延时，避免请求过快
      if (page <= maxPages) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    // 提交事务
    await db.prepare('COMMIT').run();
    
    console.log(`Collection completed! Saved ${totalSaved} videos.`);
    return { success: true, count: totalSaved };
    
  } catch (error) {
    // 回滚事务
    await db.prepare('ROLLBACK').run();
    console.error('Collection failed:', error);
    throw error;
  }
}

// 辅助函数：返回JSON响应
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type'
    }
  });
}
