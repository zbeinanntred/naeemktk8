package com.loy.e.weixin.service;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.loy.e.weixin.handler.DemoTextHandler;

import me.chanjar.weixin.common.util.StringUtils;
import me.chanjar.weixin.mp.api.WxMpConfigStorage;
import me.chanjar.weixin.mp.api.WxMpInMemoryConfigStorage;
import me.chanjar.weixin.mp.api.WxMpMessageHandler;
import me.chanjar.weixin.mp.api.WxMpMessageRouter;
import me.chanjar.weixin.mp.api.WxMpService;
import me.chanjar.weixin.mp.api.WxMpServiceImpl;
import me.chanjar.weixin.mp.bean.WxMpXmlMessage;
import me.chanjar.weixin.mp.bean.WxMpXmlOutMessage;

/**
 * 
 * 
 * @author Loy Fu qq群 540553957
 * @since 1.7
 * @version 1.0.0
 *
 */
@RestController
@RequestMapping(value="/" ,method={RequestMethod.POST,RequestMethod.GET})
public class WeixinController {
	
	protected WxMpConfigStorage wxMpConfigStorage;
	protected WxMpService wxMpService;
	protected WxMpMessageRouter wxMpMessageRouter;
	
	
	public void init(){
		WxMpInMemoryConfigStorage config = new WxMpInMemoryConfigStorage();
		//config.setAccessToken("123456");
		config.setToken("123456");
		config.setAesKey("KCNhmUxDm4umYsYj6CHIMcgQ1m6Ckj7GwogbWD9mGH5");
	    wxMpConfigStorage = config;
	    wxMpService = new WxMpServiceImpl();
	    wxMpService.setWxMpConfigStorage(config);
	    WxMpMessageHandler textHandler = new DemoTextHandler();
	    wxMpMessageRouter = new WxMpMessageRouter(wxMpService);
	    wxMpMessageRouter.rule().async(false).content("哈哈").handler(textHandler).end();
	}
	public WeixinController(){
		init();
	}
	@RequestMapping(value="/weixin") 
	public void index(HttpServletRequest request,
			HttpServletResponse response) throws IOException {

		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);

		String signature = request.getParameter("signature");
		String nonce = request.getParameter("nonce");
		String timestamp = request.getParameter("timestamp");

		if (!wxMpService.checkSignature(timestamp, nonce, signature)) {
			// 消息签名不正确，说明不是公众平台发过来的消息
			response.getWriter().println("非法请求");
			return;
		}

		String echostr = request.getParameter("echostr");
		if (StringUtils.isNotBlank(echostr)) {
			// 说明是一个仅仅用来验证的请求，回显echostr
			response.getWriter().println(echostr);
			return;
		}

		String encryptType = StringUtils.isBlank(request.getParameter("encrypt_type")) ? "raw" : request.getParameter("encrypt_type");

		if ("raw".equals(encryptType)) {
			// 明文传输的消息
			WxMpXmlMessage inMessage = WxMpXmlMessage.fromXml(request.getInputStream());
			WxMpXmlOutMessage outMessage = wxMpMessageRouter.route(inMessage);
			if (outMessage != null) {
				response.getWriter().write(outMessage.toXml());
			}
			return;
		}

		if ("aes".equals(encryptType)) {
			// 是aes加密的消息
			String msgSignature = request.getParameter("msg_signature");
			WxMpXmlMessage inMessage = WxMpXmlMessage.fromEncryptedXml(request.getInputStream(), wxMpConfigStorage, timestamp, nonce, msgSignature);
			WxMpXmlOutMessage outMessage = wxMpMessageRouter.route(inMessage);
			response.getWriter().write(outMessage.toEncryptedXml(wxMpConfigStorage));
			return;
		}

		response.getWriter().println("不可识别的加密类型");
		return;
	}
}
